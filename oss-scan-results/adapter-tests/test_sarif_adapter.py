"""Field-by-field test of the one shared SARIF adapter.

``harness/lib/normalize/adapters/sarif.py`` is the single adapter AAP 0.5.4 gives to
all three SARIF producers -- ``opengrep``, ``semgrep`` and
``datadog-static-analyzer`` -- and this module is its test. AAP 0.6.1 gives the file
its own row (*"Field-by-field assertion over the SARIF fixtures, covering the chained
uriBaseId walk with its cycle and depth guards, the metadata-backed fallback, the
first-location rule and the ascending-identifier rule"*), AAP 0.9.4 puts the
``adapter-tests/`` tree in the definition of done, and AAP 0.9.2 makes *"a failed
adapter fixture, rejection or reconciliation test"* a condition that stops the run.
A failure here is therefore a halt, not a warning.

Why the assertions are shaped the way they are
----------------------------------------------
Two of this adapter's failure modes are silent, and both produce a plausible-looking
dataset rather than an error:

* a path resolved against the wrong base still yields a well-formed row, so every
  ``in_scope`` value in the artifact can be wrong while nothing raises. That is why
  every base here is read from ``harness/artifacts/logs/runner-metadata.json``
  through the real loaders and none is written into this file as a literal;
* a rejection counted under the wrong class still balances the reconciliation
  identity. That is why each negative assertion names its
  :data:`normalize.paths.REJECT_CLASSES` member -- read from the module, never spelled
  out here -- since *a test that only counts rejections cannot tell one condition from
  another*.

The adapter contract under test
-------------------------------
The uniform entry point every adapter in the package implements::

    adapt(doc, *, tool, root, tool_base, allowlist, tally)
        -> (rows, rejections, counters)

``tool`` is one of the three canonical identifiers; ``root`` is the ``SPARK_SRC`` root
every path is expressed against; ``tool_base`` is that tool's
:class:`normalize.paths.ToolPathBase` view over the runner metadata, whose
:attr:`~normalize.paths.ToolPathBase.has_explicit_base` state **alone** decides whether
a degenerate SARIF base falls back or is rejected under ``unresolvable_path``;
``allowlist`` comes from :func:`normalize.paths.load_allowlist`; ``tally`` is fed one
:class:`normalize.severity.SeverityResult` per emitted row. ``counters`` carries the
four counts AAP 0.5.4 has reported per tool -- ``multi_location_records``,
``multi_valued_cwe_records``, ``multi_valued_cve_records`` and ``non_filesystem_paths``
-- alongside the adapter's own breakdown. ``scanner_class`` is ``"sast"`` for all three
producers, fixed by AAP 0.5.4's class table and never derived from a record.

The twenty-two required assertions, and the class that owns each
----------------------------------------------------------------
1. row count equals the expected file's ......... :class:`PositiveRowTests`
2. every row, every one of the twelve
   ``emit.FIELDS``, in order .................... :class:`PositiveRowTests`
3. canonical ``tool``; ``scanner_class`` sast ... :class:`PositiveRowTests`
4. ``rule_id`` from ``ruleId``, and from
   ``ruleIndex`` ................................ :class:`RuleIdentifierResolutionTests`
5. ``message`` from ``message.text`` ............ :class:`MessageAndSeverityTests`
6. the severity sources, with the
   ``SeverityResult`` basis ..................... :class:`MessageAndSeverityTests`
7. ``path``/``start_line`` from ``locations[0]``   :class:`FirstLocationTests`
8. ``cwe``/``cve`` from the rule's
   ``properties.cwe`` and ``tags`` .............. :class:`IdentifierSelectionTests`
9. ``package_coordinate`` absent on every row ... :class:`SchemaInvariantTests`
10. no emitted ``path`` is ever absolute ........ :class:`SchemaInvariantTests`
11. ``severity_norm``/``path`` never absent ..... :class:`SchemaInvariantTests`
12. the chain is walked, not read one level ..... :class:`UriBaseIdChainTests`
13. cycle guard, rejected under the named class   :class:`UriBaseIdChainTests`
14. depth guard, rejected under the named class   :class:`UriBaseIdChainTests`
15. base absent from the map: both branches ..... :class:`DegenerateBaseTwoBranchTests`
16. a chain with no absolute ancestor rejects ... :class:`UriBaseIdChainTests`
17. a syntactically invalid URI rejects ......... :class:`UriBaseIdChainTests`
18. ``..`` segments preserved, never normalized   :class:`ErrataConformanceTests`
19. archive ``<container>!<member>``, kept ...... :class:`ErrataConformanceTests`
20. ``ROOTPATH`` as ``file:///``: both branches   :class:`DegenerateBaseTwoBranchTests`
21. the first-location rule and its counter ..... :class:`FirstLocationTests`
22. ascending numeric CWE/CVE, and its counters   :class:`IdentifierSelectionTests`

:class:`FixtureInventoryTests` runs before all of them: a fixture silently absent, or
an expected file that failed to parse, would let every assertion above pass over an
empty loop. :class:`RowDerivationTests` then checks the per-row derivations each
expected file records -- the severity basis and selected entry, and the path basis
re-derived through :func:`normalize.paths.resolve_sarif_location` at the recorded JSON
pointer -- because a band asserted without its basis cannot show that *the absence was
stated rather than a level assumed*. :class:`RootIndependenceTests` proves rather than
repeats the root-independence the expected files claim, and
:class:`ScopeMatcherTests` exercises the ``**`` matcher on the two glob forms that
break naive implementations. :class:`NegativeFixtureTests` owns the ten rejection
conditions and :class:`AbsentPathTests` the ``absent_path`` class those ten fold into
one of their conditions. :class:`AdapterContractTests` covers the arguments the adapter
refuses -- each of which would otherwise yield a plausible dataset rather than an error
-- and :class:`ModuleHygieneTests` covers this file's own contract: standard library
only, the twelve fields, no credential-shaped literal in the source, and nothing under
``harness/lib/normalize/`` written from here.

``records walked == rows + rejections`` is not one class's business but every class's:
:meth:`SarifAdapterTestCase.assert_reconciliation_identity` counts the left side with a
traversal that builds nothing, and it is asserted wherever rows are produced -- on every
positive fixture, on every negative fixture under each of its metadata branches, and on
the authored documents. ``test_reconciliation.py`` owns the identity across the whole
dataset and ``normalize.reconcile`` with it; this module asserts it per artifact, from a
second implementation, so the two agree from independent code.

Research basis for the ``uriBaseId`` walk (primary sources)
-----------------------------------------------------------
* SARIF 2.1.0, ``https://docs.oasis-open.org/sarif/sarif/v2.1.0/sarif-v2.1.0.html``:
  section 3.4.3 (``uri``), section 3.4.4 (``uriBaseId``, which states the normative
  consumer procedure -- use a value the end user configured for the identifier if one
  exists, and otherwise resolve it from ``run.originalUriBaseIds``) and section 3.14.14
  (``originalUriBaseIds``), whose **own example expresses one base relative to
  another**. Chaining is therefore specified behaviour, not a hypothesis: a consumer
  that read one level would be wrong on conformant input, which is what assertion 12
  pins.
* Errata,
  ``https://docs.oasis-open.org/sarif/sarif/v2.1.0/errata01/os/sarif-v2.1.0-errata01-os.html``:
  issue 480 amends section 3.4.3 so that a relative reference may not begin with a
  single slash *unless* required to distinguish items in archive formats such as zip
  and tar -- the legitimate shape of an in-archive reference, which assertion 19
  requires handled rather than rejected on that ground alone; and the section 3.10.2
  amendment forbids a consumer normalizing ``..`` segments out of a path, which
  assertion 18 pins.
* Two documented producer gaps, each of which reaches the same two-branch treatment:
  a ``uriBaseId`` emitted with no matching ``originalUriBaseIds`` entry, so the
  specification's procedure cannot complete (``https://github.com/semgrep/semgrep/issues/10591``),
  and a base emitted as ``file:///`` rather than the scanned directory when the target
  is a git repository (``https://github.com/aquasecurity/trivy/issues/10364``).

The rejection conditions this adapter can produce -- and the three it cannot
----------------------------------------------------------------------------
AAP 0.5.4 enumerates the conditions; AAP 0.6.2 requires *"a negative fixture and
assertion for every rejection condition each exercised adapter can produce, present
whether or not this run's own artifacts contained that case"*, because *a rejection
path with no test is a rejection path nobody has exercised*. Producible here, one
captured fixture and one assertion each:

Every fixture below is named ``reject-sarif-<suffix>.sarif``, and the class is the
member of :data:`normalize.paths.REJECT_CLASSES` the record is counted under:

* ``unresolvable_path`` -- an unresolvable or absent path .... ``unresolvable-path``
* ``unresolvable_path`` -- the base absent from the map ...... ``uribaseid-missing-base``
* ``unresolvable_path`` -- a cyclic chain .................... ``uribaseid-cycle``
* ``unresolvable_path`` -- an over-deep chain ................ ``uribaseid-overdepth``
* ``unresolvable_path`` -- no absolute ancestor .. ``uribaseid-relative-no-absolute-ancestor``
* ``invalid_uri`` -- a syntactically invalid URI ............. ``uribaseid-invalid-uri``
* ``missing_rule_id`` ....................................... ``missing-rule-id``
* ``missing_message`` ....................................... ``missing-message``
* ``non_integer_start_line`` ................................ ``non-integer-start-line``
* ``malformed_record`` ...................................... ``malformed-record``

The four ``uriBaseId`` terminal cases and the plain unresolvable path share one class
and are distinguished by their ``detail``; :class:`NegativeFixtureTests` asserts the
detail verbatim for exactly that reason. ``absent_path`` is reachable from this shape
too -- an ``artifactLocation`` with no ``uri``, or a result with no ``locations`` --
and :class:`AbsentPathTests` covers it, so the class is exercised even though AAP
0.5.4 folds it into the same "unresolvable or absent path" condition as its fixture.

Three conditions this adapter **cannot** produce, with the reason each is absent
rather than untested:

* ``ambiguous_source_resolution`` -- it arises only where two source files claim one
  bytecode class key. A SARIF ``artifactLocation`` names a source path directly, and
  this adapter is handed no bytecode input, so there is nothing to resolve ambiguously.
  It belongs to the Joern adapter, whose input is a class file.
* ``unformable_package_coordinate`` -- ``package_coordinate`` is ``None`` on every
  SARIF row by design (AAP 0.5.4: this shape supplies no coordinate to carry), so no
  candidate level exists to fail at. It belongs to the dependency-oriented adapters.
* ``unattributable_section`` -- a record is unattributable only where its artifact has
  finding sections and the record belongs to none. SARIF has no sections: every record
  is an element of ``runs[].results[]``. It belongs to the Trivy adapter.

Hermeticity, and where each root comes from
-------------------------------------------
No test here reaches for a Spark checkout. The pinned tree is cloned outside this
repository and is neither built nor scanned from here, so a test that stat'ed a source
file would fail on a clean machine; path resolution in ``paths.py`` is string
arithmetic over the reported path and the recorded base, and touches no filesystem.
Every input this module needs is either a fixture beside it or a git-tracked file it
reads through the module's own loaders:
``harness/scope/allowlist.txt`` through :func:`normalize.paths.load_allowlist`, and
``harness/artifacts/logs/runner-metadata.json`` through
:func:`normalize.paths.load_runner_metadata`,
:func:`normalize.paths.metadata_scan_root` and
:func:`normalize.paths.tool_path_base`.

Each expected file states its own root precondition and this module honours it rather
than choosing one. Where a fixture's ``originalUriBaseIds`` names an absolute base, the
rows are root-*dependent* and the recorded scan root is what makes them hold --
``expected/reject-sarif-uribaseid-missing-base.rows.json`` says so outright:
*"Substituting a temporary directory would change the expected rows rather than reveal
a defect."* Where an expected file instead claims root-*independence*,
:class:`RootIndependenceTests` re-runs the fixture against a temporary root, with a
temporary allowlist and runner metadata written into that tree and loaded through the
same loaders, and requires the rows to come back identical -- which proves the claim
instead of restating it, and exercises the loaders on a document this module authored.

What this file deliberately does not do
---------------------------------------
It never compares the three producers. One adapter serves them, so their fixtures
appear together, but no assertion, message, comment or docstring here ranks, contrasts
or explains one tool's output against another's: AAP 0.3.2 forbids cross-tool
interpretation of any kind and AAP 0.8.2 restates it. No finding is judged real,
important, a false positive or a duplicate; nothing is deduplicated across tools; no
result is compared against Apex, Cantina or any other scanner. No secret value appears
anywhere in this file -- this tree is committed to git, since ``.gitignore:31`` ignores
only ``artifacts/`` -- and no adapter field is populated from one. Fixtures are read
and never written: :class:`FixtureInventoryTests` records each one's sha256 and
:meth:`SarifAdapterTestCase.tearDown` re-checks it, so a test that mutated one would
fail rather than pass quietly. Nothing under ``harness/lib/normalize/`` is edited from
here; a defect this file reveals there is reported, not repaired.

No user-specified rules govern this file. ``review_rules`` reports "No user rules
provided." and that one line is the whole document, corroborated independently by AAP
0.7 and AAP 0.10.2. Enterprise-standard best practice applies in their place and their
absence is expressly not licence to lower the bar -- concretely: every one of the twelve
fields is asserted individually against a hand-verified value rather than by a single
whole-dict comparison, every rejection class is asserted by name, and no assertion is
softened to make a test pass.

How to run it
-------------
From the repository root::

    python3 -m unittest discover -s oss-scan-results/adapter-tests \\
        -p 'test_sarif_adapter.py' -v

It needs no installed package, no plugin and no working directory of its own: the
standard library only (AAP 0.4.1), and AAP 0.4.3 adds no dependency in any direction.
Every path is derived from ``__file__``, so the suite behaves identically whether it is
discovered from the repository root or from anywhere else on the filesystem.
"""

from __future__ import annotations

# Standard library only, and only these eight (AAP 0.4.1; AAP 0.4.3 adds nothing in
# any direction, so there is no manifest, no lockfile and no install step):
#   ast       -- this file's own syntax tree, so "no third-party module is imported" is
#                asserted from the source rather than from a shared sys.modules;
#   hashlib   -- fixture and module digests, so "never mutate a fixture" and "never edit
#                the modules under test" are checked, not promised;
#   json      -- read a fixture and an expected file without writing either;
#   sys       -- the one-time sys.path bootstrap below;
#   tempfile  -- the temporary root that proves the root-independence claims;
#   unittest  -- the runner, so the suite needs no third-party plugin;
#   pathlib   -- every location derived from __file__ rather than from the cwd;
#   typing    -- Any, for the JSON documents this module authors and reads.
import ast
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------------------
# The one-time sys.path bootstrap.
#
# There is deliberately no __init__.py under harness/lib/normalize/: PEP 420 implicit
# namespace packages make "from normalize import paths" work once harness/lib is on
# sys.path. cli.py owns these same two lines for its own direct-script route (AAP
# 0.6.4), and this module mirrors them rather than assuming an installed package,
# because nothing installs this tree.
#
# This file sits at <repo>/oss-scan-results/adapter-tests/, so parents[2] is the
# repository root. Deriving the entry from __file__ rather than from the working
# directory is what makes the bootstrap cwd-independent. The membership guard keeps it
# idempotent: unittest discovery imports sibling test modules that perform the same
# insertion, and a duplicate path entry is noise that outlives the run in sys.path.
# --------------------------------------------------------------------------------------
_THIS_FILE = Path(__file__).resolve()
_TESTS_DIR = _THIS_FILE.parent
REPO_ROOT = _THIS_FILE.parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from normalize import emit, paths, severity  # noqa: E402  (follows the bootstrap)
from normalize.adapters import sarif  # noqa: E402  (follows the bootstrap)

# --------------------------------------------------------------------------------------
# Locations. Every one is derived from this file, never from the working directory.
# --------------------------------------------------------------------------------------

#: The captured and authored artifacts under test.
FIXTURES_DIR = _TESTS_DIR / "fixtures"

#: Each fixture's hand-verified expected outcome.
EXPECTED_DIR = _TESTS_DIR / "expected"

#: The normalizer's path-resolution input (AAP 0.6.4: written in Stage 1, read here as
#: input, and ``tool-status.md`` rendered from it afterwards -- never the reverse).
RUNNER_METADATA_PATH = REPO_ROOT / "harness" / "artifacts" / "logs" / "runner-metadata.json"

#: The scope definition, and the sole authority for the ``in_scope`` field.
ALLOWLIST_PATH = REPO_ROOT / "harness" / "scope" / "allowlist.txt"

# --------------------------------------------------------------------------------------
# The three canonical identifiers this one adapter serves, and their fixtures.
# --------------------------------------------------------------------------------------

#: Read from the adapter rather than restated, so a change there fails here loudly.
SARIF_PRODUCERS = sarif.SUPPORTED_TOOLS

#: Positive fixture stem -> the canonical tool identifier whose output it is. The stem
#: is the artifact stem, which is the canonical identifier (AAP 0.5.4: the identifier is
#: produced mechanically from the runner and artifact stem, not from a product name).
POSITIVE_FIXTURES: dict[str, str] = {
    "opengrep": "opengrep",
    "semgrep": "semgrep",
    "datadog-static-analyzer": "datadog-static-analyzer",
}

#: The ten negative fixtures, one per rejection condition this adapter can produce.
#: Every one is an ``opengrep`` artifact: the condition under test is a property of the
#: shared adapter, and one producer's shape is enough to exercise it. No comparison
#: between producers is implied or made.
NEGATIVE_FIXTURES: tuple[str, ...] = (
    "reject-sarif-unresolvable-path",
    "reject-sarif-uribaseid-missing-base",
    "reject-sarif-uribaseid-cycle",
    "reject-sarif-uribaseid-overdepth",
    "reject-sarif-uribaseid-invalid-uri",
    "reject-sarif-uribaseid-relative-no-absolute-ancestor",
    "reject-sarif-missing-rule-id",
    "reject-sarif-missing-message",
    "reject-sarif-non-integer-start-line",
    "reject-sarif-malformed-record",
)

#: The tool every negative fixture was captured from.
NEGATIVE_FIXTURE_TOOL = "opengrep"

#: The two expected files carrying a ``branches`` array rather than one outcome, because
#: the record's outcome is decided by the metadata the test supplies rather than by the
#: fixture (AAP 0.5.4's two-branch rule).
TWO_BRANCH_EXPECTATIONS: tuple[str, ...] = (
    "reject-sarif-uribaseid-missing-base",
    "reject-sarif-uribaseid-relative-no-absolute-ancestor",
)

#: The fixture suffix. SARIF artifacts carry ``.sarif``; the native shapes carry
#: ``.json`` and belong to the other adapters' tests.
FIXTURE_SUFFIX = ".sarif"

#: The expected-file suffix.
EXPECTED_SUFFIX = ".rows.json"

# --------------------------------------------------------------------------------------
# Pin-verified paths. Each names a real file at commit
# 59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d, so an expected row built on one is
# falsifiable rather than decorative. None is opened: they are reported paths, and
# resolution is arithmetic.
# --------------------------------------------------------------------------------------

#: 380 lines at the pin. Line 71 opens ``throw SparkException.internalError(``, line 72
#: is the interpolated string, line 73 is ``category = "STORAGE")``. A result at line 72
#: is factually grounded in that call rather than invented.
DISK_STORE_PATH = "core/src/main/scala/org/apache/spark/storage/DiskStore.scala"
DISK_STORE_LINE = 72
DISK_STORE_LINES_AT_THE_PIN = 380

#: 171 lines at the pin; lines 131 to 137 are the snippet the missing-base fixture
#: carries. A second in-scope path, and a ``.java`` one.
BLOCK_PUSHER_PATH = (
    "common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/"
    "OneForOneBlockPusher.java"
)
BLOCK_PUSHER_LINE = 131
BLOCK_PUSHER_LINES_AT_THE_PIN = 171

#: In scope, and ``in_scope`` **true**. One of the 832 Python test modules that sit
#: inside the authoritative glob ``python/pyspark/**`` and carry no ``src/test``
#: segment. AAP 0.3.1 makes the exclusion literal, and the loose reading of "tests are
#: out of scope" would silently flip about a fifth of the dataset -- which is what the
#: assertion on this path guards. No Spark test suite is executed by this run; the
#: module is read by scanners exactly as any other in-scope source is.
PYSPARK_TEST_MODULE_PATH = "python/pyspark/ml/tests/test_evaluation.py"

#: Matched by ``sql/connect/**/src/main/**`` -- the mid-path ``**`` form, which needs
#: true zero-or-more-segment semantics and which ``fnmatch`` and ``PurePath.match`` get
#: measurably wrong.
CONNECT_SERVER_PATH = (
    "sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/"
    "SparkConnectService.scala"
)

#: Real at the pin, inside the tree, and outside all twelve globs: a row kept with
#: ``in_scope`` false rather than dropped (AAP 0.9.3).
OUT_OF_SCOPE_PATH = "dev/package-lock.json"

#: Inside a scope glob's module but carrying the literal ``src/test``, which overrides a
#: positive glob match (AAP 0.3.1).
SRC_TEST_PATH = "core/src/test/scala/org/apache/spark/storage/DiskStoreSuite.scala"

#: The twelve authoritative scope globs, byte-exact and in the AAP 0.3.1 order, written
#: into a temporary allowlist by :meth:`SarifAdapterTestCase.hermetic_context`. Authored
#: here rather than copied out of ``paths.py`` so that the two are independent: the
#: repository's allowlist, this literal and :data:`normalize.paths.ALLOWLIST_GLOBS` are
#: required to agree, and agreement between three independently authored copies is
#: evidence, whereas one copy read twice is not. No exclusion line appears: the literal
#: ``src/test`` exclusion lives in ``paths.py``, not in the allowlist file.
AUTHORITATIVE_GLOBS: tuple[str, ...] = (
    "core/src/main/**",
    "common/network-common/src/main/**",
    "common/network-shuffle/src/main/**",
    "common/network-yarn/src/main/**",
    "sql/catalyst/src/main/**",
    "sql/core/src/main/**",
    "sql/connect/**/src/main/**",
    "sql/hive/src/main/**",
    "sql/hive-thriftserver/src/main/**",
    "resource-managers/kubernetes/**/src/main/**",
    "resource-managers/yarn/src/main/**",
    "python/pyspark/**",
)

#: The four counters AAP 0.5.4 has reported per tool. Read from the adapter's own
#: constants so a rename there fails here rather than silently dropping a check.
FOUR_REPORTED_COUNTERS: tuple[str, ...] = (
    sarif.COUNTER_MULTI_LOCATION,
    sarif.COUNTER_MULTI_VALUED_CWE,
    sarif.COUNTER_MULTI_VALUED_CVE,
    sarif.COUNTER_NON_FILESYSTEM_PATHS,
)


# --------------------------------------------------------------------------------------
# Module-level helpers.
# --------------------------------------------------------------------------------------


def _read_json(path: Path) -> Any:
    """Return the JSON document at ``path``, read-only.

    Read with an explicit encoding so the result cannot depend on the ambient locale --
    the harness pins ``C.utf8``, and a test that quietly relied on that would fail
    somewhere else. The file is never opened for writing anywhere in this module.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    """Return the hex sha256 of ``path``'s bytes.

    Used to prove the fixtures are byte-identical after the run: AAP 0.6.2's positive
    fixture is *"an unmodified captured excerpt"*, and a fixture quietly edited to suit
    an assertion tests the adapter against the shape someone believed the tool emits.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture_path(stem: str) -> Path:
    """Return the SARIF fixture named ``stem``."""
    return FIXTURES_DIR / f"{stem}{FIXTURE_SUFFIX}"


def _expected_path(stem: str) -> Path:
    """Return the hand-verified expectation for the fixture named ``stem``."""
    return EXPECTED_DIR / f"{stem}{EXPECTED_SUFFIX}"


def _json_pointer(document: Any, pointer: str) -> Any:
    """Resolve an RFC 6901 JSON pointer such as ``/runs/0/results/0`` against ``document``.

    Twelve lines rather than a dependency, because AAP 0.4.3 adds none. The pointers are
    the ones the expected files record for each row and each rejection, so resolving them
    is what turns a recorded derivation into a checked one: the test reads the very
    element the expectation claims to describe rather than the element it assumes sits at
    that index.

    Raises
    ------
    KeyError
        If the pointer does not resolve, naming the pointer and the failing token. A
        pointer that silently resolved to ``None`` would let a derivation assertion pass
        against nothing at all.
    """
    if pointer in ("", "/"):
        return document
    if not pointer.startswith("/"):
        raise KeyError(f"a JSON pointer must begin with '/'; got {pointer!r}")
    current = document
    for raw_token in pointer.split("/")[1:]:
        # RFC 6901's escapes: ~1 is '/' and ~0 is '~', and ~1 must be replaced first.
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            try:
                index = int(token)
            except ValueError as error:
                raise KeyError(
                    f"pointer {pointer!r}: token {token!r} is not an array index"
                ) from error
            if not 0 <= index < len(current):
                raise KeyError(
                    f"pointer {pointer!r}: index {index} is outside an array of "
                    f"{len(current)}"
                )
            current = current[index]
        elif isinstance(current, dict):
            if token not in current:
                raise KeyError(f"pointer {pointer!r}: no member {token!r}")
            current = current[token]
        else:
            raise KeyError(
                f"pointer {pointer!r}: token {token!r} cannot be resolved against a "
                f"{type(current).__name__}"
            )
    return current


class RecordingTally:
    """A :class:`normalize.severity.LiteralTally` that also keeps per-row order.

    The adapter feeds the tally once per **emitted row**, in row order, so the sequence
    of results recorded here is the sequence of rows -- which is the only way to check a
    per-row ``severity_basis`` and ``selected_entry`` against the derivations the expected
    files record. :class:`~normalize.severity.LiteralTally` itself aggregates on the
    ``(literal, severity_norm, basis)`` triple, so it cannot answer a per-row question
    and is not asked one.

    Every call is delegated to a real ``LiteralTally``, so the production type's own
    validation still runs -- an unknown tool identifier still raises there rather than
    being absorbed by a stand-in. The adapter sanctions the arrangement explicitly: its
    ``_validated_tally`` checks for a callable ``record`` rather than for a class, *"so a
    test double is as acceptable as a severity.LiteralTally"*.
    """

    __slots__ = ("delegate", "records")

    def __init__(self) -> None:
        self.delegate = severity.LiteralTally.with_all_tools()
        self.records: list[tuple[str, severity.SeverityResult]] = []

    def record(self, tool: str, result: severity.SeverityResult) -> None:
        """Keep ``result`` in row order, then hand it to the real tally."""
        self.records.append((tool, result))
        self.delegate.record(tool, result)

    @property
    def results(self) -> tuple[severity.SeverityResult, ...]:
        """The recorded results, in the order the adapter emitted their rows."""
        return tuple(result for _tool, result in self.records)


# --------------------------------------------------------------------------------------
# Authored SARIF documents.
#
# Ten of the twenty-two assertions concern behaviour no captured artifact exercises --
# each positive expected file names them under
# ``behaviours_not_exercised_by_this_fixture`` and says to *"cover with a derived
# fixture"*. They are authored in memory here rather than written to
# ``fixtures/``: this run creates exactly one file, and an authored document is not a
# captured artifact, so putting one in that directory beside the ten captured negatives
# would blur the distinction AAP 0.6.2 draws between them. Every authored document is a
# minimal, conformant SARIF 2.1.0 envelope -- ``version`` plus a ``runs`` array, which is
# what ``shape.py`` routes on -- carrying only the properties the assertion turns on.
# --------------------------------------------------------------------------------------


def authored_document(
    results: list[dict[str, Any]],
    *,
    rules: list[dict[str, Any]] | None = None,
    base_map: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a one-run SARIF 2.1.0 document carrying ``results``.

    Args:
        results: The ``runs[0].results`` array.
        rules: ``runs[0].tool.driver.rules``, for the rule-object routes -- a
            ``ruleIndex`` to resolve, a ``defaultConfiguration.level``, a
            ``properties.severity`` or an identifier source.
        base_map: ``runs[0].originalUriBaseIds``. Omitted entirely rather than written
            empty when ``None``, because an absent map and an empty one are different
            inputs to the walk and conflating them would weaken assertion 15.
    """
    run: dict[str, Any] = {
        "tool": {"driver": {"name": "authored-driver", "rules": rules or []}},
        "results": results,
    }
    if base_map is not None:
        run["originalUriBaseIds"] = base_map
    return {"version": "2.1.0", "runs": [run]}


def authored_result(
    uri: str | None,
    *,
    uri_base_id: str | None = None,
    start_line: int | None = DISK_STORE_LINE,
    rule_id: str | None = "authored.rule.one",
    rule_index: int | None = None,
    message: str | None = "An authored message, carrying no secret and naming no credential.",
    level: str | None = "error",
    extra_locations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return one ``runs[].results[]`` element addressing ``uri``.

    Only the properties an assertion turns on are written; anything passed as ``None`` is
    omitted from the object rather than written as JSON null, so that "the producer did
    not state it" and "the producer stated nothing" stay distinguishable -- the adapter
    treats them differently, and an assertion built on the wrong one would be vacuous.

    ``extra_locations`` appends further members after the first, which is how the
    first-location rule is exercised: the row must take ``locations[0]`` and the record
    must still count once.
    """
    artifact_location: dict[str, Any] = {}
    if uri is not None:
        artifact_location["uri"] = uri
    if uri_base_id is not None:
        artifact_location["uriBaseId"] = uri_base_id
    physical: dict[str, Any] = {"artifactLocation": artifact_location}
    if start_line is not None:
        physical["region"] = {"startLine": start_line}
    locations: list[dict[str, Any]] = [{"physicalLocation": physical}]
    if extra_locations:
        locations.extend(extra_locations)
    result: dict[str, Any] = {"message": {"text": message} if message is not None else {},
                             "locations": locations}
    if rule_id is not None:
        result["ruleId"] = rule_id
    if rule_index is not None:
        result["ruleIndex"] = rule_index
    if level is not None:
        result["level"] = level
    return result


def authored_location(uri: str, start_line: int) -> dict[str, Any]:
    """Return a further ``locations[]`` member, for the first-location rule."""
    return {
        "physicalLocation": {
            "artifactLocation": {"uri": uri},
            "region": {"startLine": start_line},
        }
    }


def count_records_independently(document: Any) -> int:
    """Count ``runs[].results[]`` elements without building anything.

    The independent half of the reconciliation identity. It reads only the containers it
    needs in order to walk -- ``runs``, then ``results``, then the length -- so an
    element it can make no sense of still counts as one record, exactly as the malformed
    fixture requires. It resolves no path, reads no message and maps no severity, which
    is the whole point: AAP 0.5.4 requires the counting traversal to walk the artifact
    *without building rows*, because *"a count taken from the same traversal that builds
    the rows satisfies the assertion while testing nothing."*

    This is deliberately a local traversal rather than a call into
    ``normalize.reconcile``: that module is not among this file's declared dependencies,
    and ``test_reconciliation.py`` owns it. Two independent implementations agreeing on
    the same fixtures is worth more here than one shared implementation asserted against
    itself.
    """
    total = 0
    runs = document.get("runs") if isinstance(document, dict) else None
    if not isinstance(runs, list):
        return 0
    for run in runs:
        if not isinstance(run, dict):
            continue
        results = run.get("results")
        if isinstance(results, list):
            total += len(results)
    return total



def expected_path_base_kind(expectation: dict[str, Any]) -> str:
    """Return the ``path_base.kind`` an expected file requires, read from the file itself.

    Each expectation records the metadata context its numbers were derived under, and the
    two spellings in use are both honoured rather than one being assumed: a
    ``resolution_context.tool_path_base`` object where the file states the whole view, and
    a ``resolution_context.path_base_kind`` scalar where it states the kind alone. The
    context is therefore an input the test *reads*, not one it chooses -- which is what
    stops a passing test from having quietly pinned whichever context the author happened
    to configure.

    Raises
    ------
    KeyError
        If the expectation records neither, since a context guessed here would make every
        path assertion in that file meaningless.
    """
    context = expectation.get("resolution_context")
    if not isinstance(context, dict):
        raise KeyError("the expectation carries no resolution_context object")
    view = context.get("tool_path_base")
    if isinstance(view, dict) and isinstance(view.get("kind"), str):
        return view["kind"]
    kind = context.get("path_base_kind")
    if isinstance(kind, str):
        return kind
    raise KeyError(
        "the expectation's resolution_context records neither tool_path_base.kind nor "
        "path_base_kind, so the metadata context its rows were derived under is unknown"
    )


class SarifAdapterTestCase(unittest.TestCase):
    """The shared harness: inputs read through the real loaders, and the field assertion.

    Class-level setup reads the three inputs every test needs -- the runner metadata, the
    scan root it records and the allowlist -- once, through
    :func:`normalize.paths.load_runner_metadata`,
    :func:`normalize.paths.metadata_scan_root` and
    :func:`normalize.paths.load_allowlist`. Going through the loaders rather than
    hand-building the structures is deliberate: a change to their shape then breaks these
    tests loudly instead of leaving them asserting against a stale hand-made stand-in.

    :meth:`tearDown` re-digests every fixture, so "never mutate a fixture" is a checked
    property of each test rather than a convention.
    """

    #: The shipped runner metadata, read once per class.
    runner_metadata: Any
    #: The scan root that document records -- ``SPARK_SRC``, read as input.
    scan_root: str
    #: The twelve globs, in file order, as the loader returns them.
    allowlist: tuple[str, ...]
    #: Fixture stem -> sha256 at class setup, re-checked after every test.
    fixture_digests: dict[str, str]

    @classmethod
    def setUpClass(cls) -> None:
        """Read the three shared inputs, and digest the fixtures."""
        super().setUpClass()
        cls.runner_metadata = paths.load_runner_metadata(RUNNER_METADATA_PATH)
        cls.scan_root = paths.metadata_scan_root(cls.runner_metadata)
        cls.allowlist = paths.load_allowlist(ALLOWLIST_PATH)
        cls.fixture_digests = {
            stem: _sha256(_fixture_path(stem))
            for stem in (*POSITIVE_FIXTURES, *NEGATIVE_FIXTURES)
        }

    def tearDown(self) -> None:
        """Fail the test that mutated a fixture, rather than let it pass quietly."""
        for stem, digest in type(self).fixture_digests.items():
            self.assertEqual(
                _sha256(_fixture_path(stem)),
                digest,
                msg=(
                    f"fixture {stem}{FIXTURE_SUFFIX} changed during this test. A fixture "
                    "is loaded, used and left byte-identical: an edited one would test "
                    "the adapter against the shape someone believed the tool emits."
                ),
            )
        super().tearDown()

    # -- inputs ------------------------------------------------------------------------

    def explicit_base(self, tool: str) -> paths.ToolPathBase:
        """Return ``tool``'s path base as the shipped metadata records it.

        ``has_explicit_base`` is true for all three SARIF producers in this provisioning,
        which is the branch under which the captured artifacts' paths resolve. The value
        is read from the document; nothing about it is written into this file.
        """
        base = paths.tool_path_base(self.runner_metadata, tool)
        self.assertTrue(
            base.has_explicit_base,
            msg=(
                f"the shipped runner metadata records no explicit base for {tool!r}, so "
                "the explicit branch cannot be exercised from it. This is a condition to "
                "report, not to repair here: nothing under harness/ is edited from a test."
            ),
        )
        return base

    def absent_base(self, tool: str) -> paths.ToolPathBase:
        """Return a path base with no explicit value, for the other branch.

        Constructed rather than obtained by editing the shipped document, which stays
        byte-exact: the branch is a property of the input a test supplies. ``kind`` is
        :data:`normalize.paths.PATH_BASE_KIND_NONE`, whose own annotation in the metadata
        vocabulary instructs that such a record be rejected under ``unresolvable_path``
        rather than fall back.
        """
        base = paths.ToolPathBase(
            tool=tool,
            kind=paths.PATH_BASE_KIND_NONE,
            base_value=None,
            scan_root=self.scan_root,
        )
        self.assertFalse(
            base.has_explicit_base,
            msg="a path base of kind 'none' with no value must not report an explicit base",
        )
        return base

    def base_of_kind(self, tool: str, kind: str) -> paths.ToolPathBase:
        """Return the path base an expectation's recorded ``kind`` names."""
        if kind == paths.PATH_BASE_KIND_NONE:
            return self.absent_base(tool)
        base = self.explicit_base(tool)
        self.assertEqual(
            base.kind,
            kind,
            msg=(
                f"the expectation was derived under path_base.kind {kind!r} but the "
                f"shipped metadata records {base.kind!r} for {tool!r}. The rows would be "
                "asserted against a context that no longer holds."
            ),
        )
        return base

    # -- invocation --------------------------------------------------------------------

    def adapt(
        self,
        document: Any,
        *,
        tool: str,
        tool_base: paths.ToolPathBase | None = None,
        root: str | None = None,
        allowlist: tuple[str, ...] | None = None,
    ) -> tuple[list[dict[str, Any]], list[paths.Rejection], dict[str, int], RecordingTally]:
        """Call the adapter under test and return its three results plus the tally.

        Defaults are the shipped context: this tool's recorded base, the recorded scan
        root and the repository's allowlist. Each is overridable so a test can vary
        exactly one input and attribute the difference to it.
        """
        tally = RecordingTally()
        rows, rejections, counters = sarif.adapt(
            document,
            tool=tool,
            root=root if root is not None else self.scan_root,
            tool_base=tool_base if tool_base is not None else self.explicit_base(tool),
            allowlist=allowlist if allowlist is not None else self.allowlist,
            tally=tally,
        )
        return rows, rejections, counters, tally

    def adapt_fixture(
        self,
        stem: str,
        *,
        tool: str,
        tool_base: paths.ToolPathBase | None = None,
        root: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[paths.Rejection], dict[str, int], RecordingTally]:
        """Run the adapter over the fixture named ``stem``."""
        return self.adapt(
            _read_json(_fixture_path(stem)), tool=tool, tool_base=tool_base, root=root
        )

    # -- assertions --------------------------------------------------------------------

    def assert_row_fields(
        self,
        actual: dict[str, Any],
        expected: dict[str, Any],
        *,
        label: str,
    ) -> None:
        """Assert one row field by field over :data:`normalize.emit.FIELDS`, in order.

        The field list is iterated from ``emit.FIELDS`` itself rather than written out
        here, so the order asserted is the authored constant's order and a field added
        there is covered without this file changing. The keys are compared as a sequence
        first -- a row carrying the right values under a different key order would satisfy
        a dict comparison and still be wrong on the wire, because ``findings.csv`` is
        written column by column in this order.

        Deliberately not ``assertEqual(actual, expected)``: a whole-dict comparison reports
        two twelve-key dicts and leaves the reader to find the difference, whereas a
        per-field assertion names the offending field in the failure message. The expected
        row's own key order is asserted too, so a drifted expected file is caught here
        rather than silently narrowing what is checked.
        """
        self.assertEqual(
            list(expected),
            list(emit.FIELDS),
            msg=f"{label}: the expected row's keys are not emit.FIELDS in order",
        )
        self.assertEqual(
            list(actual),
            list(emit.FIELDS),
            msg=(
                f"{label}: the emitted row's keys are {list(actual)!r}, not emit.FIELDS "
                "in order"
            ),
        )
        for field in emit.FIELDS:
            self.assertEqual(
                actual[field],
                expected[field],
                msg=(
                    f"{label}: field {field!r} is {actual[field]!r}, expected "
                    f"{expected[field]!r}"
                ),
            )

    def assert_rows_match(
        self,
        actual: list[dict[str, Any]],
        expected: list[dict[str, Any]],
        *,
        label: str,
    ) -> None:
        """Assert row count, then every row field by field, in document order."""
        self.assertEqual(
            len(actual),
            len(expected),
            msg=(
                f"{label}: {len(actual)} rows emitted, {len(expected)} expected. Paths "
                f"emitted: {[row.get('path') for row in actual]!r}"
            ),
        )
        for index, (emitted, wanted) in enumerate(zip(actual, expected)):
            self.assert_row_fields(emitted, wanted, label=f"{label} row {index}")

    def assert_schema_invariants(self, rows: list[dict[str, Any]], *, label: str) -> None:
        """Assert the invariants that hold for every SARIF row, whatever the fixture.

        Assertions 9 to 11, applied wherever rows are produced rather than only in the
        class that owns them: ``package_coordinate`` absent, no absolute path, and
        ``severity_norm`` and ``path`` never absent. ``emit.validate_rows`` is then run
        over the same rows, so the emitter's own contract -- which of the twelve fields may
        be absent, and its refusal of an absolute path -- is enforced by the module that
        owns it rather than re-implemented here.
        """
        for index, row in enumerate(rows):
            where = f"{label} row {index}"
            self.assertIsNone(
                row["package_coordinate"],
                msg=(
                    f"{where}: package_coordinate must be absent on every SARIF row -- a "
                    "result names a code location rather than a package, and this shape "
                    "supplies no coordinate to carry"
                ),
            )
            self.assertIsNotNone(
                row["severity_norm"], msg=f"{where}: severity_norm is never absent"
            )
            self.assertIn(
                row["severity_norm"],
                severity.SEVERITY_NORM,
                msg=f"{where}: severity_norm outside the fixed vocabulary",
            )
            self.assertIsNotNone(row["path"], msg=f"{where}: path is never absent")
            self.assertNotEqual(row["path"], "", msg=f"{where}: path is never empty")
            self.assertFalse(
                paths.is_absolute_path(row["path"]),
                msg=(
                    f"{where}: path {row['path']!r} is absolute. No absolute path is ever "
                    "emitted, including for an archive member or any other "
                    "non-filesystem coordinate."
                ),
            )
            self.assertEqual(
                row["tool"],
                row["tool"].strip(),
                msg=f"{where}: the tool identifier carries surrounding whitespace",
            )
        # The emitter validates what it will write: the same rows, through the same
        # contract findings.json and findings.csv are produced under.
        emit.validate_rows(rows)

    def assert_counters_match(
        self,
        actual: dict[str, int],
        expected: dict[str, int],
        *,
        label: str,
    ) -> None:
        """Assert every counter key and value, both directions.

        Both directions matter: a counter the adapter stopped emitting and a counter the
        expectation never knew about are different defects, and a one-directional
        comparison hides one of them.
        """
        self.assertEqual(
            sorted(actual),
            sorted(expected),
            msg=f"{label}: the counter key sets differ",
        )
        for key in sorted(expected):
            self.assertEqual(
                actual[key],
                expected[key],
                msg=f"{label}: counter {key!r} is {actual[key]}, expected {expected[key]}",
            )

    def assert_reconciliation_identity(
        self,
        document: Any,
        rows: list[dict[str, Any]],
        rejections: list[paths.Rejection],
        *,
        label: str,
        expected_records: int | None = None,
    ) -> None:
        """Assert ``records walked == rows + rejections`` against an independent count."""
        walked = count_records_independently(document)
        if expected_records is not None:
            self.assertEqual(
                walked,
                expected_records,
                msg=(
                    f"{label}: the independent traversal counted {walked} records, and the "
                    f"expectation records {expected_records}"
                ),
            )
        self.assertEqual(
            walked,
            len(rows) + len(rejections),
            msg=(
                f"{label}: {walked} records walked but {len(rows)} rows and "
                f"{len(rejections)} rejections were produced"
            ),
        )

    def assert_single_rejection(
        self,
        rejections: list[paths.Rejection],
        *,
        reject_class: str,
        label: str,
    ) -> paths.Rejection:
        """Assert exactly one rejection, under exactly ``reject_class``, and return it."""
        self.assertEqual(
            len(rejections),
            1,
            msg=(
                f"{label}: expected exactly one rejection, got "
                f"{[rejection.reject_class for rejection in rejections]!r}"
            ),
        )
        rejection = rejections[0]
        self.assertIn(
            reject_class,
            paths.REJECT_CLASSES,
            msg=(
                f"{label}: {reject_class!r} is not a member of paths.REJECT_CLASSES, so "
                "the assertion would be checking an invented spelling"
            ),
        )
        self.assertEqual(
            rejection.reject_class,
            reject_class,
            msg=(
                f"{label}: rejected under {rejection.reject_class!r}, expected "
                f"{reject_class!r}. Detail was: {rejection.detail}"
            ),
        )
        self.assertTrue(
            rejection.detail.strip(),
            msg=f"{label}: a rejection must retain a diagnostic naming the sub-reason",
        )
        return rejection

    # -- the hermetic temporary root ----------------------------------------------------

    def hermetic_context(
        self,
        tool: str,
        *,
        explicit: bool = True,
        materialise: tuple[str, ...] = (),
    ) -> tuple[str, tuple[str, ...], paths.ToolPathBase]:
        """Build a temporary root with its own allowlist and runner metadata.

        Returns ``(root, allowlist, tool_base)``, where the allowlist and the base have
        both been read back through :func:`normalize.paths.load_allowlist` and
        :func:`normalize.paths.load_runner_metadata` /
        :func:`normalize.paths.tool_path_base`. Nothing is hand-built: the loaders are the
        code under exercise here as much as the adapter is, so a change to the metadata
        schema fails these tests rather than passing over a stale structure.

        ``explicit`` selects the recorded base -- kind ``scan_root`` pointing at this
        temporary root, or kind ``none`` with no value at all. ``materialise`` creates the
        named paths as empty files inside the root, after ``mkdir(parents=True)``. The
        adapter needs none of them, since resolution is arithmetic; they are created so the
        temporary tree is a faithful stand-in for a checkout rather than a bare directory,
        and so a future resolver that did consult the filesystem would find what its
        fixture claims.

        The directory is removed by an ``addCleanup`` callback, so it goes away even where
        the test fails.
        """
        holder = tempfile.TemporaryDirectory(prefix="blitzy-sarif-adapter-test-")
        self.addCleanup(holder.cleanup)
        root = Path(holder.name).resolve()

        for relative in materialise:
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch()

        allowlist_file = root / "allowlist.txt"
        allowlist_file.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS), encoding="utf-8"
        )
        allowlist = paths.load_allowlist(allowlist_file)
        self.assertEqual(
            allowlist,
            AUTHORITATIVE_GLOBS,
            msg="the temporary allowlist did not read back as the twelve globs written",
        )

        root_text = root.as_posix()
        path_base: dict[str, Any] = (
            {"kind": paths.PATH_BASE_KIND_SCAN_ROOT, "value": root_text}
            if explicit
            else {"kind": paths.PATH_BASE_KIND_NONE, "value": None}
        )
        metadata_file = root / "runner-metadata.json"
        metadata_file.write_text(
            json.dumps(
                {
                    "purpose": (
                        "A minimal runner-metadata document authored by "
                        "test_sarif_adapter.py, so the path base reaches the adapter "
                        "through the same loader the normalizer uses."
                    ),
                    "spark_src": root_text,
                    "tools": {
                        tool: {
                            "canonical_tool_identifier": tool,
                            "scanner_class": sarif.SCANNER_CLASS,
                            "resolved_scan_root": root_text,
                            "path_base": path_base,
                        }
                    },
                },
                indent=1,
            ),
            encoding="utf-8",
        )
        document = paths.load_runner_metadata(metadata_file)
        self.assertEqual(
            paths.metadata_scan_root(document),
            root_text,
            msg="the temporary metadata did not read back the scan root written into it",
        )
        tool_base = paths.tool_path_base(document, tool)
        self.assertEqual(
            tool_base.has_explicit_base,
            explicit,
            msg=(
                "the temporary metadata's has_explicit_base does not match the branch "
                "requested"
            ),
        )
        return root_text, allowlist, tool_base



# --------------------------------------------------------------------------------------
# Inventory. This runs before every other class in alphabetical discovery order, and it
# exists because an absent fixture or an unparsable expectation would let every loop
# below iterate zero times and report success.
# --------------------------------------------------------------------------------------


class FixtureInventoryTests(SarifAdapterTestCase):
    """Every fixture and expectation this module needs is present, parsable and paired."""

    def test_every_fixture_and_expectation_exists_and_parses(self) -> None:
        """All thirteen fixtures and their expected files are present and valid JSON."""
        for stem in (*POSITIVE_FIXTURES, *NEGATIVE_FIXTURES):
            fixture = _fixture_path(stem)
            expectation = _expected_path(stem)
            with self.subTest(stem=stem):
                self.assertTrue(fixture.is_file(), msg=f"missing fixture {fixture}")
                self.assertTrue(
                    expectation.is_file(), msg=f"missing expectation {expectation}"
                )
                document = _read_json(fixture)
                self.assertIsInstance(
                    document, dict, msg=f"{stem}: a SARIF artifact's top level is an object"
                )
                self.assertEqual(
                    document.get("version"),
                    "2.1.0",
                    msg=f"{stem}: not a SARIF 2.1.0 envelope",
                )
                self.assertIsInstance(
                    document.get("runs"), list, msg=f"{stem}: no runs array"
                )
                self.assertIsInstance(
                    _read_json(expectation),
                    dict,
                    msg=f"{stem}: the expectation is not a JSON object",
                )

    def test_the_inventory_is_the_thirteen_this_module_asserts_on(self) -> None:
        """No SARIF fixture in the directory is left without an assertion.

        The direction that matters: a fixture added to ``fixtures/`` and not added to
        :data:`POSITIVE_FIXTURES` or :data:`NEGATIVE_FIXTURES` would sit there untested,
        and nothing else would notice. Enumerating the directory is what closes that.
        """
        on_disk = {
            path.name[: -len(FIXTURE_SUFFIX)]
            for path in FIXTURES_DIR.iterdir()
            if path.is_file() and path.name.endswith(FIXTURE_SUFFIX)
        }
        asserted = set(POSITIVE_FIXTURES) | set(NEGATIVE_FIXTURES)
        self.assertEqual(
            on_disk,
            asserted,
            msg=(
                "the SARIF fixtures on disk and the ones this module asserts on differ; "
                f"untested: {sorted(on_disk - asserted)!r}, missing: "
                f"{sorted(asserted - on_disk)!r}"
            ),
        )

    def test_every_positive_fixture_names_a_tool_this_adapter_serves(self) -> None:
        """The three canonical identifiers, read from the adapter's own constant."""
        self.assertEqual(
            sorted(POSITIVE_FIXTURES.values()),
            sorted(SARIF_PRODUCERS),
            msg="the positive fixtures do not cover exactly sarif.SUPPORTED_TOOLS",
        )
        for tool in SARIF_PRODUCERS:
            with self.subTest(tool=tool):
                self.assertIn(
                    tool,
                    severity.CANONICAL_TOOLS,
                    msg=f"{tool!r} is not one of the nine canonical identifiers",
                )

    def test_the_shared_inputs_read_back_as_expected(self) -> None:
        """The allowlist is the twelve authoritative globs and the scan root is absolute.

        Three independently authored copies of the scope definition are required to agree:
        the repository's ``harness/scope/allowlist.txt``, this module's
        :data:`AUTHORITATIVE_GLOBS`, and :data:`normalize.paths.ALLOWLIST_GLOBS`. Agreement
        between three copies written at different times is evidence; one copy read twice is
        not. The allowlist stays byte-exact -- it is read here and never written.
        """
        self.assertEqual(
            self.allowlist,
            AUTHORITATIVE_GLOBS,
            msg="harness/scope/allowlist.txt is not the twelve authoritative globs in order",
        )
        self.assertEqual(
            AUTHORITATIVE_GLOBS,
            paths.ALLOWLIST_GLOBS,
            msg="this module's glob literal and paths.ALLOWLIST_GLOBS disagree",
        )
        self.assertTrue(
            paths.allowlist_matches_authoritative_globs(self.allowlist),
            msg="the loaded allowlist fails paths.allowlist_matches_authoritative_globs",
        )
        self.assertTrue(
            paths.is_absolute_path(self.scan_root),
            msg=(
                "the recorded scan root must be absolute: a relative root cannot anchor "
                "anything and would produce a plausible-looking wrong answer for every row"
            ),
        )

    def test_the_reject_classes_this_module_asserts_are_real_members(self) -> None:
        """Every class name used below is a member of the closed set of ten.

        Read from the module rather than spelled out, so a rename there fails here instead
        of leaving an assertion checking a string nothing produces any more.
        """
        for name in (
            paths.REJECT_ABSENT_PATH,
            paths.REJECT_UNRESOLVABLE_PATH,
            paths.REJECT_INVALID_URI,
            paths.REJECT_MISSING_RULE_ID,
            paths.REJECT_MISSING_MESSAGE,
            paths.REJECT_NON_INTEGER_START_LINE,
            paths.REJECT_MALFORMED_RECORD,
        ):
            with self.subTest(reject_class=name):
                self.assertTrue(
                    paths.is_reject_class(name),
                    msg=f"{name!r} is not a member of paths.REJECT_CLASSES",
                )
                self.assertIn(
                    name,
                    paths.REJECT_CLASS_DESCRIPTIONS,
                    msg=f"{name!r} carries no description in paths.py",
                )

    def test_the_three_conditions_this_adapter_cannot_produce_belong_to_others(self) -> None:
        """The not-producible three exist as classes, and none is reachable from SARIF.

        Named rather than passed over. Each is a real member of the closed set -- so this
        module's docstring is not describing a class that does not exist -- and none can
        arise from a ``runs[].results[]`` element: there is no bytecode input to resolve
        ambiguously, no package coordinate to fail to form, and no finding section to be
        unattributable to. The assertion is that no fixture in this module's inventory
        produces one, which is checked against every fixture rather than argued.
        """
        not_producible = (
            paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION,
            paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE,
            paths.REJECT_UNATTRIBUTABLE_SECTION,
        )
        for name in not_producible:
            with self.subTest(reject_class=name):
                self.assertTrue(paths.is_reject_class(name))
        observed: set[str] = set()
        for stem in NEGATIVE_FIXTURES:
            expectation = _read_json(_expected_path(stem))
            kind = expected_path_base_kind(expectation)
            _rows, rejections, _counters, _tally = self.adapt_fixture(
                stem,
                tool=NEGATIVE_FIXTURE_TOOL,
                tool_base=self.base_of_kind(NEGATIVE_FIXTURE_TOOL, kind),
            )
            observed.update(rejection.reject_class for rejection in rejections)
        for name in not_producible:
            self.assertNotIn(
                name,
                observed,
                msg=(
                    f"{name!r} was produced from a SARIF artifact, which contradicts this "
                    "module's docstring; the docstring or the adapter is wrong and the "
                    "difference is a defect to report"
                ),
            )


# --------------------------------------------------------------------------------------
# Assertions 1, 2, 3 -- and 9 to 11 wherever rows appear.
# --------------------------------------------------------------------------------------


class PositiveRowTests(SarifAdapterTestCase):
    """Every row of every positive fixture, field by field, against its expected file."""

    def test_rows_match_the_expected_file_field_by_field(self) -> None:
        """Assertions 1 and 2: the row count, then all twelve fields in order.

        The context is the shipped one: this tool's recorded base and the recorded scan
        root. Each expected file states that its rows were derived under exactly that,
        and :meth:`base_of_kind` fails loudly if the metadata has since changed.
        """
        for stem, tool in POSITIVE_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            with self.subTest(fixture=stem):
                self.assertEqual(
                    expectation["tool"],
                    tool,
                    msg=f"{stem}: the expectation names a different tool",
                )
                self.assertEqual(
                    list(expectation["field_order"]),
                    list(emit.FIELDS),
                    msg=f"{stem}: the expectation's field_order is not emit.FIELDS",
                )
                rows, rejections, _counters, _tally = self.adapt_fixture(
                    stem,
                    tool=tool,
                    tool_base=self.base_of_kind(
                        tool, expected_path_base_kind(expectation)
                    ),
                )
                self.assertEqual(
                    rejections,
                    [],
                    msg=(
                        f"{stem}: a positive fixture produces no rejection; got "
                        f"{[rejection.reject_class for rejection in rejections]!r}"
                    ),
                )
                self.assert_rows_match(rows, expectation["rows"], label=stem)
                self.assertEqual(
                    len(rows),
                    expectation["counts"]["rows"],
                    msg=f"{stem}: row count disagrees with the expectation's counts block",
                )

    def test_every_row_carries_the_canonical_tool_and_the_sast_class(self) -> None:
        """Assertion 3: ``tool`` is the canonical identifier and ``scanner_class`` is sast.

        ``scanner_class`` is fixed per tool by AAP 0.5.4's class table and is never derived
        from a record's content, so it is asserted against the adapter's own constant on
        every row rather than against a literal written here.
        """
        self.assertEqual(sarif.SCANNER_CLASS, "sast")
        for stem, tool in POSITIVE_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            rows, _rejections, _counters, _tally = self.adapt_fixture(
                stem,
                tool=tool,
                tool_base=self.base_of_kind(tool, expected_path_base_kind(expectation)),
            )
            self.assertTrue(rows, msg=f"{stem}: no rows to assert on")
            for index, row in enumerate(rows):
                with self.subTest(fixture=stem, row=index):
                    self.assertEqual(row["tool"], tool)
                    self.assertEqual(row["scanner_class"], sarif.SCANNER_CLASS)

    def test_counters_match_the_expected_file_key_for_key(self) -> None:
        """Every counter the adapter returns, including the four reported per tool."""
        for stem, tool in POSITIVE_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            with self.subTest(fixture=stem):
                _rows, _rejections, counters, _tally = self.adapt_fixture(
                    stem,
                    tool=tool,
                    tool_base=self.base_of_kind(
                        tool, expected_path_base_kind(expectation)
                    ),
                )
                self.assert_counters_match(
                    counters, expectation["counters"], label=stem
                )
                for name in FOUR_REPORTED_COUNTERS:
                    self.assertIn(
                        name,
                        counters,
                        msg=f"{stem}: counter {name!r} is not reported at all",
                    )

    def test_the_reconciliation_identity_holds_on_every_positive_fixture(self) -> None:
        """``records walked == rows + rejections``, the left side counted independently."""
        for stem, tool in POSITIVE_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            document = _read_json(_fixture_path(stem))
            with self.subTest(fixture=stem):
                rows, rejections, _counters, _tally = self.adapt(
                    document,
                    tool=tool,
                    tool_base=self.base_of_kind(
                        tool, expected_path_base_kind(expectation)
                    ),
                )
                self.assert_reconciliation_identity(
                    document,
                    rows,
                    rejections,
                    label=stem,
                    expected_records=expectation["counts"]["raw_finding_records"],
                )

    def test_schema_invariants_hold_on_every_positive_row(self) -> None:
        """Assertions 9, 10 and 11 over every row of every positive fixture."""
        for stem, tool in POSITIVE_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            with self.subTest(fixture=stem):
                rows, _rejections, _counters, _tally = self.adapt_fixture(
                    stem,
                    tool=tool,
                    tool_base=self.base_of_kind(
                        tool, expected_path_base_kind(expectation)
                    ),
                )
                self.assert_schema_invariants(rows, label=stem)

    def test_the_disk_store_line_72_row_is_present_and_grounded(self) -> None:
        """The one row whose location is verified in the pinned source, not invented.

        ``DiskStore.scala`` is 380 lines at the pin; line 71 opens
        ``throw SparkException.internalError(`` and line 72 is the interpolated string
        inside that call. A row at line 72 of that file is therefore factually grounded,
        which is what makes it worth asserting individually: an off-by-one in the region
        handling would still produce a well-formed row.

        The row is asserted to exist with that path and line, and nothing is said about
        whether the finding is correct: AAP 0.3.2 forbids judging a finding real,
        important or otherwise.
        """
        expectation = _read_json(_expected_path("opengrep"))
        rows, _rejections, _counters, _tally = self.adapt_fixture(
            "opengrep",
            tool="opengrep",
            tool_base=self.base_of_kind("opengrep", expected_path_base_kind(expectation)),
        )
        matching = [
            row
            for row in rows
            if row["path"] == DISK_STORE_PATH and row["start_line"] == DISK_STORE_LINE
        ]
        self.assertEqual(
            len(matching),
            1,
            msg=(
                f"expected exactly one row at {DISK_STORE_PATH}:{DISK_STORE_LINE}; got "
                f"{len(matching)}"
            ),
        )
        row = matching[0]
        self.assertLessEqual(
            row["start_line"],
            DISK_STORE_LINES_AT_THE_PIN,
            msg="the start_line falls outside the file's length at the pin",
        )
        self.assertTrue(
            row["in_scope"],
            msg=f"{DISK_STORE_PATH} matches core/src/main/** and is in scope",
        )



# --------------------------------------------------------------------------------------
# The per-row derivations each expected file records. A band asserted without its basis
# cannot show that the absence was stated rather than a level assumed, and a path
# asserted without its basis cannot show which of the four resolution routes produced it.
# --------------------------------------------------------------------------------------


class RowDerivationTests(SarifAdapterTestCase):
    """Each recorded derivation is re-derived and checked, not taken on trust."""

    def _run(
        self, stem: str
    ) -> tuple[
        dict[str, Any],
        list[dict[str, Any]],
        dict[str, int],
        RecordingTally,
        list[dict[str, Any]],
    ]:
        """Return ``(document, rows, counters, tally, derivations)`` for one fixture."""
        tool = POSITIVE_FIXTURES[stem]
        expectation = _read_json(_expected_path(stem))
        document = _read_json(_fixture_path(stem))
        rows, rejections, counters, tally = self.adapt(
            document,
            tool=tool,
            tool_base=self.base_of_kind(tool, expected_path_base_kind(expectation)),
        )
        self.assertEqual(rejections, [], msg=f"{stem}: unexpected rejection")
        derivations = expectation["row_derivations"]
        self.assertEqual(
            len(derivations),
            len(rows),
            msg=f"{stem}: {len(derivations)} derivations recorded for {len(rows)} rows",
        )
        return document, rows, counters, tally, derivations

    @staticmethod
    def _run_index_of(result_pointer: str) -> int:
        """Return the run index a ``/runs/<n>/results/<m>`` pointer names."""
        return int(result_pointer.split("/")[2])

    def test_severity_basis_and_selected_entry_per_row(self) -> None:
        """Assertion 6's second half: the basis, and the entry that was used.

        The tally is fed once per emitted row, in row order, so the recorded results line
        up with the rows one for one. Asserting the basis is what makes "the absence was
        stated rather than a level assumed" checkable: two rows can carry the same band
        for entirely different reasons -- a mapped label, an unmapped literal disclosed, or
        no vocabulary at all -- and only the basis distinguishes them.
        """
        for stem in POSITIVE_FIXTURES:
            _document, rows, _counters, tally, derivations = self._run(stem)
            self.assertEqual(
                len(tally.results),
                len(rows),
                msg=f"{stem}: the tally was not fed exactly once per emitted row",
            )
            for index, (row, derivation, result) in enumerate(
                zip(rows, derivations, tally.results)
            ):
                with self.subTest(fixture=stem, row=index):
                    self.assertEqual(
                        derivation["row_index"],
                        index,
                        msg="the derivation records a different row index",
                    )
                    self.assertEqual(
                        result.basis,
                        derivation["severity_basis"],
                        msg=(
                            f"{stem} row {index}: severity basis {result.basis!r}, "
                            f"expected {derivation['severity_basis']!r}"
                        ),
                    )
                    self.assertIn(
                        result.basis,
                        severity.BASIS_VALUES,
                        msg="the basis is outside severity.BASIS_VALUES",
                    )
                    self.assertEqual(
                        result.severity_native,
                        row["severity_native"],
                        msg="the row's severity_native is not the tallied literal",
                    )
                    self.assertEqual(
                        result.severity_norm,
                        row["severity_norm"],
                        msg="the row's severity_norm is not the tallied band",
                    )
                    if "severity_selected_entry" in derivation:
                        self.assertEqual(
                            result.selected_entry,
                            derivation["severity_selected_entry"],
                            msg=(
                                "the selected entry -- what was actually used to reach "
                                "the band -- differs from the recorded one"
                            ),
                        )
                    if result.basis == severity.BASIS_NO_VOCABULARY:
                        self.assertIsNone(
                            row["severity_native"],
                            msg=(
                                "basis no_vocabulary requires an absent severity_native: "
                                "the absence is stated, never filled in"
                            ),
                        )
                    if result.basis == severity.BASIS_UNMAPPED_LITERAL:
                        self.assertEqual(
                            result.unmapped_literal,
                            row["severity_native"],
                            msg="an unmapped literal must be disclosed verbatim",
                        )
                        self.assertEqual(row["severity_norm"], severity.INFO)

    def test_the_two_severity_decompositions_sum_to_the_counters(self) -> None:
        """Each row's recorded severity counter and basis add up to the counter block.

        Two decompositions of the same rows: ``severity_from_*`` counts which **source**
        supplied the literal, and ``severity_basis_*`` counts how the **band** was reached.
        They differ by design, and summing the per-row records against both is what shows
        the counters describe these rows rather than some other set.
        """
        for stem in POSITIVE_FIXTURES:
            _document, rows, counters, tally, derivations = self._run(stem)
            with self.subTest(fixture=stem):
                from_source: dict[str, int] = {}
                from_basis: dict[str, int] = {}
                for derivation, result in zip(derivations, tally.results):
                    name = derivation["severity_counter"]
                    from_source[name] = from_source.get(name, 0) + 1
                    key = f"{sarif.COUNTER_SEVERITY_BASIS_PREFIX}{result.basis}"
                    from_basis[key] = from_basis.get(key, 0) + 1
                for name, total in from_source.items():
                    self.assertEqual(
                        counters[name],
                        total,
                        msg=f"{stem}: counter {name!r} is {counters[name]}, rows say {total}",
                    )
                for key, total in from_basis.items():
                    self.assertEqual(
                        counters[key],
                        total,
                        msg=f"{stem}: counter {key!r} is {counters[key]}, rows say {total}",
                    )
                self.assertEqual(
                    sum(from_source.values()),
                    len(rows),
                    msg="every emitted row must be accounted for by exactly one source",
                )

    def test_path_basis_and_kind_re_derived_through_the_resolver(self) -> None:
        """The recorded ``path_basis`` and ``path_kind``, re-derived from the fixture.

        The adapter's rows carry the resolved path but not the basis it was established
        on, so the basis is re-derived by handing the very same inputs -- the
        ``artifactLocation`` at the recorded pointer and the enclosing run's base map -- to
        :func:`normalize.paths.resolve_sarif_location`, and requiring the path it returns
        to be the path the row carries. That equality is what makes the re-derivation a
        check on the row rather than a separate calculation beside it.
        """
        for stem in POSITIVE_FIXTURES:
            tool = POSITIVE_FIXTURES[stem]
            expectation = _read_json(_expected_path(stem))
            base = self.base_of_kind(tool, expected_path_base_kind(expectation))
            document, rows, counters, _tally, derivations = self._run(stem)
            kinds: dict[str, int] = {}
            for index, (row, derivation) in enumerate(zip(rows, derivations)):
                pointer = derivation["result_pointer"]
                run = document["runs"][self._run_index_of(pointer)]
                artifact_location = _json_pointer(document, derivation["path_source"])
                # path_source points at the uri itself; the location object is its parent.
                location = _json_pointer(
                    document, pointer + "/locations/0/physicalLocation/artifactLocation"
                )
                self.assertEqual(
                    location.get(paths.SARIF_URI_KEY),
                    artifact_location,
                    msg=f"{stem} row {index}: path_source does not point at the uri",
                )
                resolved = paths.resolve_sarif_location(
                    location.get(paths.SARIF_URI_KEY),
                    location.get(paths.SARIF_URI_BASE_ID_KEY),
                    run.get(paths.SARIF_ORIGINAL_URI_BASE_IDS_KEY),
                    self.scan_root,
                    base,
                    tool=tool,
                )
                with self.subTest(fixture=stem, row=index):
                    self.assertIsInstance(
                        resolved,
                        paths.ResolvedPath,
                        msg=f"re-resolution produced {resolved!r}",
                    )
                    self.assertEqual(
                        resolved.path,
                        row["path"],
                        msg="the re-derived path is not the path the row carries",
                    )
                    self.assertEqual(
                        resolved.basis,
                        derivation["path_basis"],
                        msg=(
                            f"path basis {resolved.basis!r}, expected "
                            f"{derivation['path_basis']!r}"
                        ),
                    )
                    self.assertEqual(
                        resolved.kind,
                        derivation["path_kind"],
                        msg=(
                            f"path kind {resolved.kind!r}, expected "
                            f"{derivation['path_kind']!r}"
                        ),
                    )
                kinds[resolved.kind] = kinds.get(resolved.kind, 0) + 1
            with self.subTest(fixture=stem, aggregate="path_kind counters"):
                for kind, total in kinds.items():
                    key = f"{sarif.COUNTER_PATH_KIND_PREFIX}{kind}"
                    self.assertEqual(
                        counters[key],
                        total,
                        msg=f"{stem}: counter {key!r} is {counters[key]}, rows say {total}",
                    )

    def test_recorded_field_sources_resolve_to_the_emitted_values(self) -> None:
        """Assertions 5 and 7: each pointer-valued source resolves to the field's value.

        Where a derivation names its source as a JSON pointer, the pointed element must be
        the value the row carries. A prose source -- "the result's own ruleId" -- is left
        to the class that owns that field, since a pointer is the only form a test can
        follow mechanically.
        """
        for stem in POSITIVE_FIXTURES:
            document, rows, _counters, _tally, derivations = self._run(stem)
            for index, (row, derivation) in enumerate(zip(rows, derivations)):
                for key, field in (
                    ("message_source", "message"),
                    ("severity_native_source", "severity_native"),
                    ("start_line_source", "start_line"),
                ):
                    source = derivation.get(key)
                    if not (isinstance(source, str) and source.startswith("/")):
                        continue
                    with self.subTest(fixture=stem, row=index, source=key):
                        self.assertEqual(
                            _json_pointer(document, source),
                            row[field],
                            msg=(
                                f"{stem} row {index}: {key} {source!r} does not resolve to "
                                f"the emitted {field!r}"
                            ),
                        )

    def test_locations_in_record_and_the_matched_glob(self) -> None:
        """The recorded location count, and the glob the row's ``in_scope`` came from.

        ``matched_allowlist_glob`` is re-derived through
        :func:`normalize.paths.matches_any_glob` rather than compared as a label, so the
        recorded glob is the one the matcher actually returns for that path -- and the
        multi-location counter is checked against the recorded counts in the same pass.
        """
        for stem in POSITIVE_FIXTURES:
            document, rows, counters, _tally, derivations = self._run(stem)
            multi = 0
            for index, (row, derivation) in enumerate(zip(rows, derivations)):
                pointer = derivation["result_pointer"]
                locations = _json_pointer(document, pointer + "/locations")
                with self.subTest(fixture=stem, row=index):
                    self.assertEqual(
                        len(locations),
                        derivation["locations_in_record"],
                        msg="the recorded location count is not the fixture's",
                    )
                    matched = paths.matches_any_glob(row["path"], self.allowlist)
                    self.assertEqual(
                        matched,
                        derivation["matched_allowlist_glob"],
                        msg=(
                            f"{row['path']!r} matched {matched!r}, the derivation records "
                            f"{derivation['matched_allowlist_glob']!r}"
                        ),
                    )
                    self.assertEqual(
                        row["in_scope"],
                        matched is not None and not paths.contains_src_test(row["path"]),
                        msg="in_scope does not follow from the allowlist match",
                    )
                if len(locations) > 1:
                    multi += 1
            with self.subTest(fixture=stem, aggregate="multi_location_records"):
                self.assertEqual(
                    counters[sarif.COUNTER_MULTI_LOCATION],
                    multi,
                    msg="the multi-location counter disagrees with the fixture's records",
                )

    def test_distinct_identifier_counts_agree_with_the_counters(self) -> None:
        """The recorded distinct CWE/CVE counts, and the two multi-valued counters.

        A record with more than one identifier still emits one value and still counts once;
        the counter is how the multi-valued records are reported per tool, so it is checked
        against the per-row records rather than assumed.
        """
        for stem in POSITIVE_FIXTURES:
            _document, rows, counters, _tally, derivations = self._run(stem)
            multi_cwe = 0
            multi_cve = 0
            for index, (row, derivation) in enumerate(zip(rows, derivations)):
                cwe_count = derivation["distinct_cwe_identifiers"]
                cve_count = derivation["distinct_cve_identifiers"]
                with self.subTest(fixture=stem, row=index):
                    if cwe_count == 0:
                        self.assertIsNone(
                            row["cwe"],
                            msg="no identifier was found, so the field must be absent",
                        )
                    else:
                        self.assertIsNotNone(row["cwe"])
                    if cve_count == 0:
                        self.assertIsNone(row["cve"])
                    else:
                        self.assertIsNotNone(row["cve"])
                if cwe_count > 1:
                    multi_cwe += 1
                if cve_count > 1:
                    multi_cve += 1
            with self.subTest(fixture=stem, aggregate="multi-valued counters"):
                self.assertEqual(counters[sarif.COUNTER_MULTI_VALUED_CWE], multi_cwe)
                self.assertEqual(counters[sarif.COUNTER_MULTI_VALUED_CVE], multi_cve)



# --------------------------------------------------------------------------------------
# Assertion 4 -- the two routes to a rule identifier.
# --------------------------------------------------------------------------------------


class RuleIdentifierResolutionTests(SarifAdapterTestCase):
    """``rule_id`` from ``ruleId``, and from ``ruleIndex`` where ``ruleId`` is absent."""

    def test_both_routes_are_exercised_by_the_captured_fixtures(self) -> None:
        """A captured artifact covers each route, and the counters say which was taken.

        SARIF 2.1.0 lets a result identify its rule either directly or by index into
        ``runs[].tool.driver.rules[]``. Both are live somewhere in the captured fixtures,
        and the adapter counts them separately, so the counters are the evidence that each
        route was actually taken rather than one route serving twice.

        The two counters are summed across the fixtures rather than read per fixture, and
        deliberately so: the question is whether the adapter's two routes are exercised at
        all, not which artifact exercises which. Reading them per tool would invite a
        comparison between producers, and this module makes none.
        """
        totals = {sarif.COUNTER_RULE_ID_FROM_RULE_ID: 0,
                  sarif.COUNTER_RULE_ID_FROM_RULE_INDEX: 0}
        for stem, tool in POSITIVE_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            _rows, _rejections, counters, _tally = self.adapt_fixture(
                stem,
                tool=tool,
                tool_base=self.base_of_kind(tool, expected_path_base_kind(expectation)),
            )
            for key in totals:
                totals[key] += counters[key]
        for key, total in totals.items():
            with self.subTest(counter=key):
                self.assertGreater(
                    total,
                    0,
                    msg=(
                        f"no captured fixture exercises {key!r}; one of the two rule "
                        "identifier routes is untested"
                    ),
                )

    def test_rule_id_comes_from_the_result_where_it_states_one(self) -> None:
        """The direct route: ``ruleId`` on the result is the identifier, verbatim."""
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.direct.rule")],
            rules=[{"id": "authored.indexed.rule"}],
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["rule_id"], "authored.direct.rule")
        self.assertEqual(counters[sarif.COUNTER_RULE_ID_FROM_RULE_ID], 1)
        self.assertEqual(counters[sarif.COUNTER_RULE_ID_FROM_RULE_INDEX], 0)
        self.assert_schema_invariants(rows, label="authored direct ruleId")

    def test_rule_id_resolves_through_rule_index_where_rule_id_is_absent(self) -> None:
        """The indexed route: ``ruleIndex`` reaches the rule object's ``id``.

        The rules array carries two entries so that the index has to be used rather than
        the first entry happening to be right -- a resolver that ignored the index and took
        ``rules[0]`` would pass against a single-entry array.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id=None, rule_index=1)],
            rules=[{"id": "authored.rule.zero"}, {"id": "authored.rule.one"}],
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["rule_id"],
            "authored.rule.one",
            msg="the identifier must come from the rule the index names",
        )
        self.assertEqual(counters[sarif.COUNTER_RULE_ID_FROM_RULE_INDEX], 1)
        self.assertEqual(counters[sarif.COUNTER_RULE_ID_FROM_RULE_ID], 0)

    def test_neither_route_available_is_a_counted_rejection(self) -> None:
        """No ``ruleId`` and an unusable ``ruleIndex``: ``missing_rule_id``, counted.

        ``rule_id`` is one of the seven fields that is never absent, so a record with no
        obtainable identifier is rejected rather than emitted with an empty one.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id=None, rule_index=7)],
            rules=[{"id": "authored.rule.zero"}],
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_MISSING_RULE_ID,
            label="authored out-of-range ruleIndex",
        )


# --------------------------------------------------------------------------------------
# Assertions 5 and 6 -- the message, and the four severity sources with their basis.
# --------------------------------------------------------------------------------------


class MessageAndSeverityTests(SarifAdapterTestCase):
    """``message`` from ``message.text``; severity from the first source that states one."""

    def test_message_comes_from_message_text(self) -> None:
        """Assertion 5, on an authored result whose text is unmistakable."""
        text = "An authored message naming no credential and quoting no secret."
        document = authored_document([authored_result(DISK_STORE_PATH, message=text)])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(rows[0]["message"], text)

    def test_a_result_level_outranks_the_rules_own_severity(self) -> None:
        """The result's ``level`` is the first source, and an earlier source outranks a later.

        Reaching past a literal the producer stated would be inference, so the rule's
        ``properties.severity`` is not consulted at all where the result states a level.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", level="note")],
            rules=[{"id": "authored.rule.one", "properties": {"severity": "CRITICAL"}}],
        )
        rows, _rejections, counters, tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows[0]["severity_native"], "note")
        self.assertEqual(rows[0]["severity_norm"], "Low")
        self.assertEqual(tally.results[0].basis, severity.BASIS_SARIF_LEVEL)
        self.assertEqual(counters[sarif.COUNTER_SEVERITY_FROM_LEVEL], 1)
        self.assertEqual(counters[sarif.COUNTER_SEVERITY_FROM_RULE_PROPERTY], 0)

    def test_the_sarif_level_table_maps_all_four_literals(self) -> None:
        """Assertion 6's fixed map: error High, warning Medium, note Low, none Info.

        The table is read from :func:`normalize.severity.sarif_level_table` as well as
        asserted end to end through the adapter, so the mapping is checked both where it is
        authored and where it is applied.
        """
        expected_bands = {
            "error": "High",
            "warning": "Medium",
            "note": "Low",
            "none": "Info",
        }
        self.assertEqual(
            severity.sarif_level_table(),
            expected_bands,
            msg="the SARIF level table is not the fixed four-literal map",
        )
        document = authored_document(
            [
                authored_result(DISK_STORE_PATH, level=level)
                for level in expected_bands
            ]
        )
        rows, rejections, _counters, tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), len(expected_bands))
        for row, (level, band) in zip(rows, expected_bands.items()):
            with self.subTest(level=level):
                self.assertEqual(row["severity_native"], level)
                self.assertEqual(row["severity_norm"], band)
        for result in tally.results:
            self.assertEqual(result.basis, severity.BASIS_SARIF_LEVEL)

    def test_the_rules_default_configuration_level_is_the_second_source(self) -> None:
        """A result stating no level takes the level its own rule states.

        SARIF's own derivation for a result that omits the property, rather than an
        inference about it -- so the basis is still the level table's.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", level=None)],
            rules=[
                {
                    "id": "authored.rule.one",
                    "defaultConfiguration": {"level": "warning"},
                }
            ],
        )
        rows, _rejections, counters, tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows[0]["severity_native"], "warning")
        self.assertEqual(rows[0]["severity_norm"], "Medium")
        self.assertEqual(tally.results[0].basis, severity.BASIS_SARIF_LEVEL)
        self.assertEqual(
            counters[sarif.COUNTER_SEVERITY_FROM_RULE_DEFAULT_CONFIGURATION], 1
        )

    def test_a_rule_property_severity_is_the_third_source(self) -> None:
        """``properties.severity`` on the rule, mapped through the label vocabulary.

        The label lookup strips and upper-cases the observed literal while
        ``severity_native`` keeps it exactly as observed, so a mixed-case label maps and is
        still reported verbatim.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", level=None)],
            rules=[{"id": "authored.rule.one", "properties": {"severity": "Moderate"}}],
        )
        rows, _rejections, counters, tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows[0]["severity_native"], "Moderate")
        self.assertEqual(rows[0]["severity_norm"], "Medium")
        self.assertEqual(tally.results[0].basis, severity.BASIS_LABEL)
        self.assertEqual(tally.results[0].selected_entry, {"label": "Moderate"})
        self.assertEqual(counters[sarif.COUNTER_SEVERITY_FROM_RULE_PROPERTY], 1)

    def test_a_rule_problem_severity_is_the_fourth_source(self) -> None:
        """``properties.problem.severity`` is consulted where the three before it are silent."""
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", level=None)],
            rules=[
                {
                    "id": "authored.rule.one",
                    "properties": {"problem": {"severity": "LOW"}},
                }
            ],
        )
        rows, _rejections, counters, tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows[0]["severity_native"], "LOW")
        self.assertEqual(rows[0]["severity_norm"], "Low")
        self.assertEqual(tally.results[0].basis, severity.BASIS_LABEL)
        self.assertEqual(counters[sarif.COUNTER_SEVERITY_FROM_RULE_PROPERTY], 1)

    def test_a_literal_outside_every_mapped_vocabulary_is_disclosed_as_info(self) -> None:
        """An unmapped literal bands Info and is kept verbatim, never guessed at.

        The band comes from policy rather than from the literal, which is why
        ``selected_entry`` is ``None`` on this path: nothing was used to derive it. The
        literal is disclosed in both ``severity_native`` and ``unmapped_literal`` so
        ``severity-map.md`` can list it with the rows it affected.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", level=None)],
            rules=[
                {
                    "id": "authored.rule.one",
                    "properties": {"severity": "AUTHORED-UNMAPPED-LABEL"},
                }
            ],
        )
        rows, _rejections, counters, tally = self.adapt(document, tool="opengrep")
        result = tally.results[0]
        self.assertEqual(rows[0]["severity_native"], "AUTHORED-UNMAPPED-LABEL")
        self.assertEqual(rows[0]["severity_norm"], severity.INFO)
        self.assertEqual(result.basis, severity.BASIS_UNMAPPED_LITERAL)
        self.assertEqual(result.unmapped_literal, "AUTHORED-UNMAPPED-LABEL")
        self.assertIsNone(
            result.selected_entry,
            msg="nothing was used to derive the band, so no entry may be recorded",
        )
        self.assertEqual(
            counters[f"{sarif.COUNTER_SEVERITY_BASIS_PREFIX}{severity.BASIS_UNMAPPED_LITERAL}"],
            1,
        )

    def test_all_four_sources_silent_states_the_absence(self) -> None:
        """No level anywhere: ``severity_native`` absent, band Info, basis no_vocabulary.

        The absence is stated rather than a level assumed. ``severity_native`` is one of
        the five fields absence is permitted for; ``severity_norm`` is not, and is Info by
        policy.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", level=None)],
            rules=[{"id": "authored.rule.one", "properties": {}}],
        )
        rows, _rejections, counters, tally = self.adapt(document, tool="opengrep")
        self.assertIsNone(rows[0]["severity_native"])
        self.assertEqual(rows[0]["severity_norm"], severity.INFO)
        self.assertEqual(tally.results[0].basis, severity.BASIS_NO_VOCABULARY)
        self.assertEqual(counters[sarif.COUNTER_SEVERITY_ABSENT], 1)
        self.assert_schema_invariants(rows, label="authored no-vocabulary")

    def test_a_missing_message_is_a_counted_rejection(self) -> None:
        """``message`` is never absent, so a record without one is rejected and counted."""
        document = authored_document([authored_result(DISK_STORE_PATH, message=None)])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_MISSING_MESSAGE,
            label="authored empty message object",
        )



# --------------------------------------------------------------------------------------
# Assertions 7 and 21 -- the first location, and the record that carries several.
# --------------------------------------------------------------------------------------


class FirstLocationTests(SarifAdapterTestCase):
    """``path`` and ``start_line`` from ``locations[0]``, and the first-location rule."""

    def test_path_and_start_line_come_from_the_first_locations_physical_location(self) -> None:
        """Assertion 7: the ``artifactLocation.uri`` and the ``region.startLine``."""
        document = authored_document(
            [authored_result(BLOCK_PUSHER_PATH, start_line=BLOCK_PUSHER_LINE)]
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(rows[0]["path"], BLOCK_PUSHER_PATH)
        self.assertEqual(rows[0]["start_line"], BLOCK_PUSHER_LINE)
        self.assertLessEqual(
            rows[0]["start_line"],
            BLOCK_PUSHER_LINES_AT_THE_PIN,
            msg="the line falls outside the file's length at the pin",
        )

    def test_a_record_with_several_locations_yields_one_row_from_the_first(self) -> None:
        """Assertion 21: one row, taking ``locations[0]``; one record; the counter moves.

        Three locations rather than two, so a row built from the *last* location is
        distinguished from one built from the first as clearly as a row built from the
        second is. The counter increments by exactly one -- it counts multi-location
        **records**, not the extra locations they carry.
        """
        document = authored_document(
            [
                authored_result(
                    DISK_STORE_PATH,
                    start_line=DISK_STORE_LINE,
                    extra_locations=[
                        authored_location(PYSPARK_TEST_MODULE_PATH, 5),
                        authored_location(OUT_OF_SCOPE_PATH, 1),
                    ],
                )
            ]
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(
            len(rows), 1, msg="a record with several locations still produces one row"
        )
        self.assertEqual(rows[0]["path"], DISK_STORE_PATH)
        self.assertEqual(rows[0]["start_line"], DISK_STORE_LINE)
        self.assertEqual(
            counters[sarif.COUNTER_MULTI_LOCATION],
            1,
            msg="the multi-location counter counts records, once each",
        )
        self.assert_reconciliation_identity(
            document, rows, rejections, label="authored multi-location", expected_records=1
        )

    def test_the_multi_location_counter_stays_at_zero_for_a_single_location(self) -> None:
        """The counter is not incremented for the ordinary one-location record.

        The other half of assertion 21: a counter that incremented for every record would
        satisfy the test above while reporting a figure that means nothing.
        """
        document = authored_document([authored_result(DISK_STORE_PATH)])
        _rows, _rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(counters[sarif.COUNTER_MULTI_LOCATION], 0)

    def test_an_absent_region_leaves_start_line_absent_rather_than_rejected(self) -> None:
        """``start_line`` is one of the five fields absence is permitted for.

        A physical location with no region names a file but no line, which is a row with an
        absent ``start_line`` -- not a rejection, and not a fabricated line 1.
        """
        document = authored_document([authored_result(DISK_STORE_PATH, start_line=None)])
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertIsNone(rows[0]["start_line"])
        self.assertEqual(counters[sarif.COUNTER_START_LINE_ABSENT], 1)
        self.assertIn("start_line", emit.OPTIONAL_FIELDS)
        self.assert_schema_invariants(rows, label="authored absent region")

    def test_a_non_integer_start_line_is_a_counted_rejection(self) -> None:
        """A ``startLine`` present but not an integer is rejected under its own class.

        Present-but-wrong is not the same as absent: absent is permitted and rejecting it
        would drop a legitimate row, while coercing a non-integer would invent a line the
        producer never stated.
        """
        document = authored_document([authored_result(DISK_STORE_PATH, start_line=None)])
        document["runs"][0]["results"][0]["locations"][0]["physicalLocation"]["region"] = {
            "startLine": "seventy-two"
        }
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_NON_INTEGER_START_LINE,
            label="authored string startLine",
        )


# --------------------------------------------------------------------------------------
# Assertions 8 and 22 -- the identifier sources, and the ascending-numeric rule.
# --------------------------------------------------------------------------------------


class IdentifierSelectionTests(SarifAdapterTestCase):
    """``cwe``/``cve`` from the rule's ``properties.cwe`` and ``tags``, smallest first."""

    def _one_row(self, rule_properties: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
        """Adapt one authored result whose rule carries ``rule_properties``."""
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one")],
            rules=[{"id": "authored.rule.one", "properties": rule_properties}],
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 1)
        return rows[0], counters

    def test_identifiers_are_collected_from_properties_cwe_and_from_tags(self) -> None:
        """Assertion 8: both sources on the rule are read, in that order.

        ``properties.cwe`` accepts a scalar or a list; ``properties.tags`` supplies the rest.
        Each is exercised on its own so that a resolver reading only one of them fails here
        rather than passing on a fixture that happens to carry both.
        """
        row, _counters = self._one_row({"cwe": "CWE-502"})
        self.assertEqual(row["cwe"], "CWE-502", msg="a scalar properties.cwe must be read")

        row, _counters = self._one_row({"cwe": ["CWE-611"]})
        self.assertEqual(row["cwe"], "CWE-611", msg="a list properties.cwe must be read")

        row, _counters = self._one_row(
            {"tags": ["security", "CWE-79: Cross-site Scripting", "CVE-2022-1234"]}
        )
        self.assertEqual(row["cwe"], "CWE-79", msg="a tag carrying a CWE token must be read")
        self.assertEqual(row["cve"], "CVE-2022-1234")

    def test_a_rule_with_no_identifier_leaves_both_fields_absent(self) -> None:
        """Absence is permitted for ``cwe`` and ``cve``, and is not filled in."""
        row, counters = self._one_row({"tags": ["security", "correctness"]})
        self.assertIsNone(row["cwe"])
        self.assertIsNone(row["cve"])
        self.assertEqual(counters[sarif.COUNTER_MULTI_VALUED_CWE], 0)
        self.assertEqual(counters[sarif.COUNTER_MULTI_VALUED_CVE], 0)

    def test_a_bare_number_is_not_read_as_a_cwe_identifier(self) -> None:
        """``79`` is not ``CWE-79``: supplying the prefix would be inference.

        Reject rather than infer, applied to a field rather than to a record: the field is
        absent because the artifact never wrote an identifier, and the row is still emitted.
        """
        row, _counters = self._one_row({"cwe": 79})
        self.assertIsNone(
            row["cwe"],
            msg="a bare number must not be turned into an identifier the artifact never wrote",
        )

    def test_the_smallest_cwe_wins_where_producer_and_lexicographic_order_disagree(self) -> None:
        """Assertion 22 for CWE: ascending **numeric**, not producer and not lexicographic.

        ``CWE-100`` is listed first and is also the lexicographic minimum, since ``'1'``
        sorts before ``'8'``; the numeric minimum is ``CWE-89``. One fixture therefore
        distinguishes all three orderings at once, which is what makes the assertion able
        to fail: with the pair the other way round, producer order and numeric order would
        agree and the test would pass under either implementation.
        """
        candidates = ["CWE-100", "CWE-89"]
        self.assertEqual(
            sorted(candidates)[0],
            "CWE-100",
            msg="the fixture no longer distinguishes lexicographic from numeric order",
        )
        row, counters = self._one_row({"tags": candidates})
        self.assertEqual(
            row["cwe"],
            "CWE-89",
            msg=(
                "the emitted CWE must be the smallest numeric identifier; CWE-100 is both "
                "the producer's first and the lexicographic minimum"
            ),
        )
        self.assertEqual(
            counters[sarif.COUNTER_MULTI_VALUED_CWE],
            1,
            msg="a record carrying more than one CWE is counted once",
        )

    def test_the_smallest_cve_wins_by_year_then_sequence(self) -> None:
        """Assertion 22 for CVE: ordered by year, then by sequence.

        Three pairs, each isolating one property of the ordering:

        * ``CVE-2021-44228`` against ``CVE-2021-4104`` -- the pair AAP 0.5.4's own example
          names, where the sequence is what decides and the longer sequence is the larger
          number rather than the later one;
        * ``CVE-2019-12384`` against ``CVE-2019-9999`` -- where lexicographic and numeric
          order genuinely **disagree**, so a string comparison fails here;
        * ``CVE-2020-1`` against ``CVE-2019-99999`` -- where the year outranks a far larger
          sequence, so an implementation comparing sequences first fails here.
        """
        row, counters = self._one_row({"tags": ["CVE-2021-44228", "CVE-2021-4104"]})
        self.assertEqual(row["cve"], "CVE-2021-4104")
        self.assertEqual(counters[sarif.COUNTER_MULTI_VALUED_CVE], 1)

        candidates = ["CVE-2019-12384", "CVE-2019-9999"]
        self.assertEqual(
            sorted(candidates)[0],
            "CVE-2019-12384",
            msg="the fixture no longer distinguishes lexicographic from numeric order",
        )
        row, _counters = self._one_row({"tags": candidates})
        self.assertEqual(
            row["cve"],
            "CVE-2019-9999",
            msg=(
                "the emitted CVE must be the smallest by sequence; CVE-2019-12384 is the "
                "lexicographic minimum"
            ),
        )

        row, _counters = self._one_row({"tags": ["CVE-2020-1", "CVE-2019-99999"]})
        self.assertEqual(
            row["cve"],
            "CVE-2019-99999",
            msg="the year outranks the sequence, however much larger the sequence is",
        )

    def test_the_ordering_is_total_so_no_tiebreak_is_needed(self) -> None:
        """The same identifier written twice collapses to one distinct value.

        The ordering is total over the integers, so no tie can arise and no producer-order
        tiebreak may be introduced. Two spellings of one identifier are one identifier, and
        the multi-valued counter must therefore stay at zero.
        """
        row, counters = self._one_row(
            {"cwe": "CWE-89", "tags": ["CWE-89", "cwe-89 lower case spelling"]}
        )
        self.assertEqual(row["cwe"], "CWE-89")
        self.assertEqual(
            counters[sarif.COUNTER_MULTI_VALUED_CWE],
            0,
            msg="one identifier written several ways is still one identifier",
        )

    def test_a_captured_fixture_exercises_the_multi_valued_counter(self) -> None:
        """The multi-valued path is reached from captured output, not only authored input.

        The authored documents above pin the ordering; this checks that the same counter
        moves on a real captured artifact, so the behaviour is not reachable only from input
        this module wrote itself. Which of the fixtures carries such a record is a property
        of that captured excerpt and is asserted from its own expected file -- the fixture
        is located by scanning the inventory rather than named here, and no property is
        attributed to a producer.
        """
        carrying = [
            (stem, tool, expectation)
            for stem, tool in POSITIVE_FIXTURES.items()
            for expectation in (_read_json(_expected_path(stem)),)
            if expectation["counters"][sarif.COUNTER_MULTI_VALUED_CVE] > 0
        ]
        self.assertTrue(
            carrying,
            msg=(
                "no captured fixture records a multi-valued CVE any more, so the ordering "
                "rule is exercised only by authored input"
            ),
        )
        for stem, tool, expectation in carrying:
            with self.subTest(fixture=stem):
                _rows, _rejections, counters, _tally = self.adapt_fixture(
                    stem,
                    tool=tool,
                    tool_base=self.base_of_kind(
                        tool, expected_path_base_kind(expectation)
                    ),
                )
                self.assertEqual(
                    counters[sarif.COUNTER_MULTI_VALUED_CVE],
                    expectation["counters"][sarif.COUNTER_MULTI_VALUED_CVE],
                )
                self.assertGreater(counters[sarif.COUNTER_MULTI_VALUED_CVE], 0)



# --------------------------------------------------------------------------------------
# Assertions 9 to 11 -- the schema invariants, on input chosen to stress them.
# --------------------------------------------------------------------------------------


class SchemaInvariantTests(SarifAdapterTestCase):
    """The twelve fields, the five absences permitted, and the paths never emitted."""

    def test_package_coordinate_is_absent_on_every_row_this_adapter_emits(self) -> None:
        """Assertion 9, over every fixture as well as authored input.

        A SARIF result names a code location rather than a package, so there is no
        coordinate to carry. The field is present-with-null rather than omitted, because the
        twelve keys are the schema and a row missing one would not round-trip through
        ``findings.csv``.
        """
        for stem, tool in POSITIVE_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            rows, _rejections, _counters, _tally = self.adapt_fixture(
                stem,
                tool=tool,
                tool_base=self.base_of_kind(tool, expected_path_base_kind(expectation)),
            )
            for index, row in enumerate(rows):
                with self.subTest(fixture=stem, row=index):
                    self.assertIn("package_coordinate", row)
                    self.assertIsNone(row["package_coordinate"])
        self.assertIn("package_coordinate", emit.OPTIONAL_FIELDS)

    def test_only_the_five_permitted_fields_may_be_absent(self) -> None:
        """Assertion 11, stated against the emitter's own constant.

        ``severity_native``, ``start_line``, ``cwe``, ``cve`` and ``package_coordinate`` --
        and nothing else. ``path`` and ``severity_norm`` are named in AAP 0.8.2 as never
        absent, and the other five required fields are required because the absence
        convention has no room for them: an empty CSV cell means null.
        """
        self.assertEqual(
            emit.OPTIONAL_FIELDS,
            frozenset(
                {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
            ),
        )
        self.assertNotIn("path", emit.OPTIONAL_FIELDS)
        self.assertNotIn("severity_norm", emit.OPTIONAL_FIELDS)
        document = authored_document(
            [
                authored_result(
                    DISK_STORE_PATH,
                    rule_id="authored.rule.one",
                    level=None,
                    start_line=None,
                )
            ],
            rules=[{"id": "authored.rule.one", "properties": {}}],
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        row = rows[0]
        for field in emit.FIELDS:
            with self.subTest(field=field):
                if field in emit.OPTIONAL_FIELDS:
                    continue
                self.assertIsNotNone(
                    row[field], msg=f"required field {field!r} came back absent"
                )
        self.assertIsNone(row["severity_native"])
        self.assertIsNone(row["start_line"])
        self.assertIsNone(row["cwe"])
        self.assertIsNone(row["cve"])
        self.assertIsNone(row["package_coordinate"])

    def test_no_emitted_path_is_ever_absolute(self) -> None:
        """Assertion 10, on the four shapes most likely to leak an absolute path.

        A ``file:`` URI, a filesystem-absolute reference, a location outside the root and an
        archive container are each relativized rather than passed through, and the emitter
        would raise on an absolute value in any case -- which is asserted here by running
        every row through :func:`normalize.emit.validate_rows`.
        """
        document = authored_document(
            [
                authored_result(f"file://{self.scan_root}/{DISK_STORE_PATH}"),
                authored_result(f"{self.scan_root}/{BLOCK_PUSHER_PATH}"),
                authored_result("file:///opt/authored-other-tree/Foo.java"),
                authored_result("jar:core/src/main/authored.jar!/org/apache/Foo.class"),
            ]
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 4)
        self.assert_schema_invariants(rows, label="authored absolute-path shapes")
        self.assertEqual(rows[0]["path"], DISK_STORE_PATH)
        self.assertEqual(rows[1]["path"], BLOCK_PUSHER_PATH)

    def test_the_emitter_refuses_an_absolute_path_so_the_invariant_is_enforced(self) -> None:
        """The invariant is enforced by the emitter, not merely observed by this test.

        A row hand-built with an absolute path must be refused. Without this, assertion 10
        would only be saying that the fixtures happen not to contain one.
        """
        row = {field: None for field in emit.FIELDS}
        row.update(
            {
                "tool": "opengrep",
                "scanner_class": sarif.SCANNER_CLASS,
                "rule_id": "authored.rule.one",
                "message": "An authored message.",
                "severity_norm": severity.INFO,
                "path": f"{self.scan_root}/{DISK_STORE_PATH}",
                "in_scope": False,
            }
        )
        with self.assertRaises(emit.EmitError):
            emit.validate_rows([row])


# --------------------------------------------------------------------------------------
# The ``in_scope`` matcher. Three globs depend on ``**`` meaning zero or more
# directories, and ``fnmatch`` and ``PurePath.match`` do not provide those semantics.
# --------------------------------------------------------------------------------------


class ScopeMatcherTests(SarifAdapterTestCase):
    """``in_scope`` decided by the allowlist alone, through a matcher with true ``**``."""

    def test_the_glob_forms_that_break_naive_implementations(self) -> None:
        """A mid-path ``**`` and a trailing ``**``, each asserted on a real pin path.

        ``sql/connect/**/src/main/**`` is the form that needs ``**`` to match zero or more
        whole segments in the middle of a pattern, and ``python/pyspark/**`` the form that
        needs it to match many segments at the end. Getting either wrong drops whole
        modules silently, and a silently dropped module looks exactly like a module with
        nothing to report.
        """
        document = authored_document(
            [
                authored_result(CONNECT_SERVER_PATH),
                authored_result(PYSPARK_TEST_MODULE_PATH),
                authored_result(DISK_STORE_PATH),
                authored_result(OUT_OF_SCOPE_PATH),
                authored_result(SRC_TEST_PATH),
            ]
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        by_path = {row["path"]: row for row in rows}

        self.assertTrue(
            by_path[CONNECT_SERVER_PATH]["in_scope"],
            msg=(
                "a mid-path ** must match one or more intervening segments: "
                "sql/connect/**/src/main/** covers sql/connect/server/src/main/..."
            ),
        )
        self.assertTrue(
            by_path[DISK_STORE_PATH]["in_scope"],
            msg="core/src/main/** covers a deeply nested path under it",
        )
        self.assertFalse(
            by_path[OUT_OF_SCOPE_PATH]["in_scope"],
            msg=f"{OUT_OF_SCOPE_PATH} is inside the tree and outside all twelve globs",
        )
        self.assertIn(
            OUT_OF_SCOPE_PATH,
            by_path,
            msg="an out-of-scope row is kept with in_scope false, never dropped",
        )
        self.assertEqual(
            counters[sarif.COUNTER_ROWS_OUT_OF_SCOPE],
            2,
            msg="both the out-of-glob path and the src/test path are out of scope",
        )
        self.assertEqual(counters[sarif.COUNTER_ROWS_IN_SCOPE], 3)

    def test_a_python_test_module_inside_the_glob_is_in_scope(self) -> None:
        """The assertion that guards about a fifth of the dataset.

        ``python/pyspark/ml/tests/test_evaluation.py`` is one of the 832 Python test modules
        that sit inside the authoritative glob ``python/pyspark/**`` and carry no
        ``src/test`` segment. The exclusion is literal, so it removes every Scala and Java
        test tree and removes nothing from ``python/pyspark/**``. A loose reading of "tests
        are out of scope" would flip all of these to false, and nothing else in the
        pipeline would notice.

        No Spark test suite is executed by this run: the module is a path in an artifact
        here, read exactly as any other in-scope source is.
        """
        self.assertFalse(
            paths.contains_src_test(PYSPARK_TEST_MODULE_PATH),
            msg="the path carries no src/test segment, so the exclusion cannot apply",
        )
        document = authored_document([authored_result(PYSPARK_TEST_MODULE_PATH, start_line=1)])
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertIs(
            rows[0]["in_scope"],
            True,
            msg=f"{PYSPARK_TEST_MODULE_PATH} is inside python/pyspark/** and is in scope",
        )
        self.assertEqual(
            paths.matches_any_glob(PYSPARK_TEST_MODULE_PATH, self.allowlist),
            "python/pyspark/**",
        )
        self.assertEqual(counters[sarif.COUNTER_ROWS_IN_SCOPE], 1)

    def test_a_jvm_test_tree_path_is_out_of_scope_and_kept(self) -> None:
        """The ordinary Scala and Java test-tree shape: out of scope, and still emitted.

        ``core/src/test/...`` is out of scope on both counts -- it matches none of the
        twelve globs, all of which name ``src/main``, and it carries the literal
        ``src/test``. Both are asserted, so the row's ``in_scope`` is not attributed to the
        wrong one of the two. The row is kept: nothing is ever filtered on ``in_scope``.
        """
        self.assertIsNone(
            paths.matches_any_glob(SRC_TEST_PATH, self.allowlist),
            msg="the twelve globs all name src/main, so a src/test path matches none",
        )
        self.assertTrue(paths.contains_src_test(SRC_TEST_PATH))
        document = authored_document([authored_result(SRC_TEST_PATH, start_line=1)])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertIs(rows[0]["in_scope"], False)
        self.assertEqual(
            rows[0]["path"], SRC_TEST_PATH, msg="the row is kept, not filtered out"
        )

    def test_the_src_test_exclusion_overrides_a_positive_glob_match(self) -> None:
        """A path matching a glob **and** containing ``src/test`` is out of scope.

        The override needs a path where the two rules disagree, and the only glob that can
        produce one is ``python/pyspark/**``, since it matches everything beneath that
        directory. The path below is authored for exactly that purpose rather than taken
        from the pin -- it is asserted to match the glob, so the override is what decides
        the outcome and not the absence of a match.
        """
        overridden = "python/pyspark/sql/src/test/authored_helper.py"
        self.assertEqual(
            paths.matches_any_glob(overridden, self.allowlist),
            "python/pyspark/**",
            msg="the authored path must match a glob for the override to be under test",
        )
        self.assertTrue(paths.contains_src_test(overridden))
        document = authored_document([authored_result(overridden, start_line=1)])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertIs(
            rows[0]["in_scope"],
            False,
            msg="the literal src/test exclusion overrides a positive glob match",
        )
        self.assertEqual(
            rows[0]["path"], overridden, msg="the row is kept, not filtered out"
        )

    def test_in_scope_is_a_boolean_and_the_matcher_is_asked_once_per_row(self) -> None:
        """``in_scope`` is a real ``bool``, since the CSV column holds only true and false.

        A truthy value that was not a ``bool`` would render as something other than the two
        literals the column admits.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH), authored_result(OUT_OF_SCOPE_PATH)]
        )
        rows, _rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        for index, row in enumerate(rows):
            with self.subTest(row=index):
                self.assertIsInstance(row["in_scope"], bool)



# --------------------------------------------------------------------------------------
# Assertions 12, 13, 14, 16 and 17 -- the bounded chain walk and its terminal cases.
#
# Every base map below is derived from the recorded scan root rather than written with a
# literal directory in it, so the chain reconstructs whatever root the metadata records
# and the assertions do not depend on that root's spelling.
# --------------------------------------------------------------------------------------


class UriBaseIdChainTests(SarifAdapterTestCase):
    """The chain is followed; each way it can fail is classified rather than collapsed."""

    def _root_parts(self) -> tuple[str, str]:
        """Return ``(parent, name)`` of the recorded scan root, for building a chain."""
        trimmed = self.scan_root.rstrip("/")
        parent, _, name = trimmed.rpartition("/")
        self.assertTrue(
            name, msg=f"the recorded scan root {self.scan_root!r} has no final segment"
        )
        return parent or "", name

    def _chained_base_map(self) -> dict[str, Any]:
        """A conformant two-level map: ``SRCROOT`` expressed relative to ``PROJECTROOT``.

        This is the shape SARIF 2.1.0 section 3.14.14's own example uses. ``PROJECTROOT``
        is absolute and ``SRCROOT`` is the root's final segment beneath it, so a walk that
        follows the chain reconstructs the recorded scan root exactly, while a walk that
        read one level would stop on the relative ``"<name>/"``.
        """
        parent, name = self._root_parts()
        return {
            "SRCROOT": {"uri": f"{name}/", "uriBaseId": "PROJECTROOT"},
            "PROJECTROOT": {"uri": f"file://{parent}/"},
        }

    def test_the_chain_is_walked_rather_than_read_one_level(self) -> None:
        """Assertion 12: a base expressed relative to another base resolves correctly.

        Two things are asserted, and the second is what makes the first meaningful. The walk
        reports both identifiers in its chain, in order, so more than one level was
        followed; and the row's path comes back as the reference itself, which is only true
        if the accumulated relative level was joined beneath the absolute ancestor.

        A one-level implementation fails both: its base would be the relative ``"<name>/"``,
        which has no absolute ancestor, and the record would be rejected instead of
        emitted. Chaining is specified behaviour -- the specification's own example does it
        -- so being wrong here means being wrong on conformant input.
        """
        base_map = self._chained_base_map()
        walk = paths.resolve_uri_base("SRCROOT", base_map)
        self.assertEqual(walk.outcome, paths.BASE_OUTCOME_RESOLVED)
        self.assertEqual(
            walk.chain,
            ("SRCROOT", "PROJECTROOT"),
            msg="the walk did not follow the chain through both identifiers",
        )
        self.assertEqual(
            walk.base,
            self.scan_root.rstrip("/"),
            msg="following the chain must reconstruct the recorded scan root",
        )

        document = authored_document(
            [authored_result(DISK_STORE_PATH, uri_base_id="SRCROOT")], base_map=base_map
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(
            rejections,
            [],
            msg=(
                "a conformant chained base must resolve; a rejection here is the "
                "one-level reading failing on input the specification blesses"
            ),
        )
        self.assertEqual(rows[0]["path"], DISK_STORE_PATH)
        self.assertTrue(rows[0]["in_scope"])
        self.assert_schema_invariants(rows, label="authored chained base")

    def test_the_chain_basis_is_the_walks_own_and_not_the_metadata_fallback(self) -> None:
        """A completed walk records its own basis, distinct from the fallback's.

        Two different provenances reach the same path, and only the basis distinguishes
        them: the specification's procedure completing on its own evidence, and the
        runner-recorded base standing in where it could not. Collapsing them would hide
        which one a row depended on.
        """
        base_map = self._chained_base_map()
        resolved = paths.resolve_sarif_location(
            DISK_STORE_PATH,
            "SRCROOT",
            base_map,
            self.scan_root,
            self.explicit_base("opengrep"),
            tool="opengrep",
        )
        self.assertIsInstance(resolved, paths.ResolvedPath)
        self.assertEqual(resolved.basis, paths.BASIS_SARIF_BASE_CHAIN)
        self.assertNotEqual(resolved.basis, paths.BASIS_SARIF_METADATA_BASE)

    def test_a_cyclic_chain_terminates_and_is_rejected(self) -> None:
        """Assertion 13: the visited-identifier set stops the walk; the record is rejected.

        The walk is bounded twice over, so the assertion is on the **outcome** rather than
        merely on the walk returning. Without the visited set the depth bound would still
        end this chain, and the record would still be rejected -- but it would be reported
        as an over-deep chain, and a reader would go looking for a chain of nine where the
        defect is a loop of three. Attributing the termination to the guard that caught it
        is what the outcome and the recorded chain carry, and what this asserts.

        The rejection is asserted with the metadata's explicit base **absent**, because a
        cycle is one of the four outcomes eligible for the documented fallback: with an
        explicit base the record legitimately resolves, and asserting a rejection there
        would be asserting the fallback away.
        """
        base_map = {
            "BASE_A": {"uri": "a/", "uriBaseId": "BASE_B"},
            "BASE_B": {"uri": "b/", "uriBaseId": "BASE_C"},
            "BASE_C": {"uri": "c/", "uriBaseId": "BASE_A"},
        }
        walk = paths.resolve_uri_base("BASE_A", base_map)
        self.assertEqual(walk.outcome, paths.BASE_OUTCOME_CYCLE)
        self.assertIsNone(walk.base)
        self.assertEqual(
            walk.chain,
            ("BASE_A", "BASE_B", "BASE_C"),
            msg="the chain must record the identifiers visited before the repeat",
        )
        self.assertIn(
            paths.BASE_OUTCOME_CYCLE,
            paths.BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK,
            msg="a cycle is one of the outcomes the documented fallback covers",
        )

        document = authored_document(
            [authored_result(DISK_STORE_PATH, uri_base_id="BASE_A")], base_map=base_map
        )
        rows, rejections, _counters, _tally = self.adapt(
            document, tool="opengrep", tool_base=self.absent_base("opengrep")
        )
        self.assertEqual(rows, [])
        rejection = self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_UNRESOLVABLE_PATH,
            label="authored cyclic chain",
        )
        self.assertIn(
            "BASE_A",
            rejection.detail,
            msg="the detail must name the identifier the chain revisited",
        )

    def test_an_over_deep_chain_terminates_and_is_rejected(self) -> None:
        """Assertion 14: the depth guard stops a chain that never repeats an identifier.

        A separate guard from the cycle guard and separately necessary: this chain visits a
        new identifier every time, so the visited set never fires, and only the depth bound
        ends it. The bound is read from
        :data:`normalize.paths.SARIF_BASE_CHAIN_MAX_DEPTH` rather than written here, and the
        chain is built three links longer than whatever it is.
        """
        depth = paths.SARIF_BASE_CHAIN_MAX_DEPTH
        self.assertGreater(depth, 1, msg="a bound of one would forbid the specified chaining")
        parent, _name = self._root_parts()
        links = depth + 3
        base_map: dict[str, Any] = {
            f"BASE_{index:02d}": {
                "uri": f"level{index}/",
                "uriBaseId": f"BASE_{index + 1:02d}",
            }
            for index in range(links)
        }
        base_map[f"BASE_{links:02d}"] = {"uri": f"file://{parent}/"}
        walk = paths.resolve_uri_base("BASE_00", base_map)
        self.assertEqual(walk.outcome, paths.BASE_OUTCOME_OVER_DEPTH)
        self.assertIsNone(walk.base)
        self.assertEqual(
            len(set(walk.chain)),
            len(walk.chain),
            msg="no identifier repeats, so the visited set cannot be what stopped this walk",
        )
        self.assertIn(
            paths.BASE_OUTCOME_OVER_DEPTH,
            paths.BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK,
        )

        document = authored_document(
            [authored_result(DISK_STORE_PATH, uri_base_id="BASE_00")], base_map=base_map
        )
        rows, rejections, _counters, _tally = self.adapt(
            document, tool="opengrep", tool_base=self.absent_base("opengrep")
        )
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_UNRESOLVABLE_PATH,
            label="authored over-deep chain",
        )

    def test_the_specified_two_level_chain_is_within_the_bound(self) -> None:
        """The bound stops a defect without disabling the chaining the specification requires.

        The other half of assertion 14: a guard set so tight that it rejected conformant
        input would pass the test above and be wrong. The specified two-level chain must
        still resolve.
        """
        walk = paths.resolve_uri_base("SRCROOT", self._chained_base_map())
        self.assertEqual(walk.outcome, paths.BASE_OUTCOME_RESOLVED)
        self.assertLessEqual(len(walk.chain), paths.SARIF_BASE_CHAIN_MAX_DEPTH)

    def test_a_chain_with_no_absolute_ancestor_is_rejected_under_both_branches(self) -> None:
        """Assertion 16: a chain ending on a relative reference is rejected, unconditionally.

        This outcome is deliberately **not** among the four the documented fallback covers,
        so it is rejected whether or not the metadata supplies an explicit base. Asserting
        both branches is what pins the order of the two gates: the eligibility gate returns
        before the metadata gate is consulted, and the observable evidence for that is the
        detail carrying no clause about ``path_base.kind`` even in the branch where an
        explicit base exists.
        """
        base_map = {
            "SRCROOT_REL": {"uri": "spark-src/", "uriBaseId": "PROJECT_REL"},
            "PROJECT_REL": {"uri": "project/"},
        }
        walk = paths.resolve_uri_base("SRCROOT_REL", base_map)
        self.assertEqual(walk.outcome, paths.BASE_OUTCOME_NO_ABSOLUTE_ANCESTOR)
        self.assertNotIn(
            paths.BASE_OUTCOME_NO_ABSOLUTE_ANCESTOR,
            paths.BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK,
            msg="this outcome must not be routed through the documented fallback",
        )
        self.assertFalse(walk.eligible_for_metadata_fallback)

        document = authored_document(
            [authored_result(DISK_STORE_PATH, uri_base_id="SRCROOT_REL")],
            base_map=base_map,
        )
        for label, base in (
            ("explicit base", self.explicit_base("opengrep")),
            ("absent base", self.absent_base("opengrep")),
        ):
            with self.subTest(branch=label):
                rows, rejections, _counters, _tally = self.adapt(
                    document, tool="opengrep", tool_base=base
                )
                self.assertEqual(rows, [])
                rejection = self.assert_single_rejection(
                    rejections,
                    reject_class=paths.REJECT_UNRESOLVABLE_PATH,
                    label=f"authored relative-terminal chain, {label}",
                )
                self.assertNotIn(
                    "path_base.kind",
                    rejection.detail,
                    msg=(
                        "the detail naming path_base.kind would mean the metadata gate had "
                        "been reached, which it must not be for an ineligible outcome"
                    ),
                )

    def test_a_syntactically_invalid_base_uri_is_rejected_under_its_own_class(self) -> None:
        """Assertion 17: an invalid URI is ``invalid_uri``, not ``unresolvable_path``.

        A distinct class because it means something different about the producer: the
        reference is malformed rather than unanchorable, and no base could rescue it. Both
        branches reject, and the class is the one thing that separates this from the four
        base-machinery cases that share ``unresolvable_path``.
        """
        base_map = {"BROKEN": {"uri": "file:///opt/\x01broken/"}}
        walk = paths.resolve_uri_base("BROKEN", base_map)
        self.assertEqual(walk.outcome, paths.BASE_OUTCOME_INVALID_URI)
        self.assertNotIn(
            paths.BASE_OUTCOME_INVALID_URI,
            paths.BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK,
        )
        document = authored_document(
            [authored_result(DISK_STORE_PATH, uri_base_id="BROKEN")], base_map=base_map
        )
        for label, base in (
            ("explicit base", self.explicit_base("opengrep")),
            ("absent base", self.absent_base("opengrep")),
        ):
            with self.subTest(branch=label):
                rows, rejections, _counters, _tally = self.adapt(
                    document, tool="opengrep", tool_base=base
                )
                self.assertEqual(rows, [])
                self.assert_single_rejection(
                    rejections,
                    reject_class=paths.REJECT_INVALID_URI,
                    label=f"authored invalid base uri, {label}",
                )

    def test_an_invalid_reported_uri_is_rejected_under_the_same_class(self) -> None:
        """The reported reference itself can be invalid, not only a base entry's."""
        document = authored_document([authored_result("core/src/main/scala/\x01A.scala")])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_INVALID_URI,
            label="authored invalid reported uri",
        )

    def test_a_valid_reference_naming_nothing_in_the_tree_is_unresolvable(self) -> None:
        """A foreign scheme is valid and still names no location under the root.

        Valid-but-unanchorable and invalid are different conditions with different classes,
        and both are asserted so that neither absorbs the other.
        """
        for uri in ("http://example.invalid/Foo.java", "urn:uuid:0000-authored"):
            with self.subTest(uri=uri):
                document = authored_document([authored_result(uri)])
                rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
                self.assertEqual(rows, [])
                self.assert_single_rejection(
                    rejections,
                    reject_class=paths.REJECT_UNRESOLVABLE_PATH,
                    label=f"authored foreign scheme {uri}",
                )


# --------------------------------------------------------------------------------------
# Assertions 15 and 20 -- the two documented producer gaps, and the one condition that
# decides each record's fate.
# --------------------------------------------------------------------------------------


class DegenerateBaseTwoBranchTests(SarifAdapterTestCase):
    """A degenerate base resolves or is rejected, decided by the metadata alone."""

    def _assert_two_branches(
        self,
        document: dict[str, Any],
        *,
        label: str,
        expected_path: str = DISK_STORE_PATH,
    ) -> None:
        """Assert the same document resolves under an explicit base and is rejected without one.

        The fixture is identical in both halves and exactly one input varies, so the
        difference is attributable to that input and to nothing else. This is the shape AAP
        0.5.4 insists on: the documented degenerate-base fallback *"applies where the
        metadata makes the base known, and everywhere else the record is rejected and
        counted rather than guessed."*
        """
        rows, rejections, counters, _tally = self.adapt(
            document, tool="opengrep", tool_base=self.explicit_base("opengrep")
        )
        self.assertEqual(
            rejections, [], msg=f"{label}: an explicit base must rescue the record"
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], expected_path)
        self.assert_schema_invariants(rows, label=f"{label} (explicit base)")
        self.assert_reconciliation_identity(
            document, rows, rejections, label=f"{label} (explicit base)"
        )
        resolved_counters = counters

        rows, rejections, counters, _tally = self.adapt(
            document, tool="opengrep", tool_base=self.absent_base("opengrep")
        )
        self.assertEqual(
            rows,
            [],
            msg=(
                f"{label}: with no explicit base the record must be rejected, never "
                "resolved through a guess"
            ),
        )
        rejection = self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_UNRESOLVABLE_PATH,
            label=f"{label} (absent base)",
        )
        self.assertIn(
            "path_base.kind",
            rejection.detail,
            msg=(
                f"{label}: the detail must record that the metadata supplied no explicit "
                "base, since that is what decided the outcome"
            ),
        )
        self.assertIn(
            paths.PATH_BASE_KIND_NONE,
            rejection.detail,
            msg=f"{label}: the detail must name the base kind it found",
        )
        self.assert_reconciliation_identity(
            document, rows, rejections, label=f"{label} (absent base)"
        )
        self.assertEqual(
            sorted(resolved_counters),
            sorted(counters),
            msg=f"{label}: the counter vocabulary must not depend on the branch",
        )

    def test_a_base_identifier_absent_from_the_map_takes_both_branches(self) -> None:
        """Assertion 15: the documented producer gap, and the fallback that is not a catch-all.

        A ``uriBaseId`` with no matching ``originalUriBaseIds`` entry is a real, reported
        producer behaviour, so the specification's procedure cannot complete. The map is
        present and non-empty but names a different identifier, which is the case that
        matters: an implementation treating an absent map and an absent entry differently
        would pass on one and fail on the other.
        """
        base_map = {"SOME_OTHER_BASE": {"uri": f"file://{self.scan_root}/"}}
        walk = paths.resolve_uri_base("%REPOSITORYROOT%", base_map)
        self.assertEqual(walk.outcome, paths.BASE_OUTCOME_ABSENT)
        self.assertTrue(walk.eligible_for_metadata_fallback)
        document = authored_document(
            [authored_result(DISK_STORE_PATH, uri_base_id="%REPOSITORYROOT%")],
            base_map=base_map,
        )
        self._assert_two_branches(document, label="base absent from a non-empty map")

    def test_a_uri_base_id_with_no_map_at_all_takes_both_branches(self) -> None:
        """The same treatment where the producer emits no ``originalUriBaseIds`` whatever.

        This is the shape the captured artifacts in this tree carry, so the branch that
        rescues them is the one their expected rows were derived under -- and the branch
        that rejects is what those rows would become under metadata recording no base.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, uri_base_id="%SRCROOT%")], base_map=None
        )
        self._assert_two_branches(document, label="uriBaseId with no map")

    def test_rootpath_emitted_as_a_bare_file_uri_takes_both_branches(self) -> None:
        """Assertion 20: a base of ``file:///`` is degenerate, and gets the same treatment.

        The second documented producer gap: ``file:///`` decodes to the filesystem root, so
        a consumer that accepted it as a base would relativize every path in the artifact
        against ``/`` and produce a long ``../`` chain for each -- rows that look
        well-formed and are wrong throughout. It is therefore classified as degenerate and
        routed through the same two branches rather than used.
        """
        base_map = {"ROOTPATH": {"uri": "file:///"}}
        walk = paths.resolve_uri_base("ROOTPATH", base_map)
        self.assertEqual(walk.outcome, paths.BASE_OUTCOME_DEGENERATE)
        self.assertTrue(walk.eligible_for_metadata_fallback)
        document = authored_document(
            [authored_result(DISK_STORE_PATH, uri_base_id="ROOTPATH")], base_map=base_map
        )
        self._assert_two_branches(document, label="degenerate ROOTPATH base")

    def test_the_fallback_records_its_own_basis_so_it_is_never_silent(self) -> None:
        """A rescued record says so: the basis names the fallback, not the walk.

        The fallback is documented rather than hidden, and the basis is where that shows up
        on a row-by-row level -- without it, a rescued row would be indistinguishable from
        one whose base map worked.
        """
        resolved = paths.resolve_sarif_location(
            DISK_STORE_PATH,
            "%SRCROOT%",
            None,
            self.scan_root,
            self.explicit_base("opengrep"),
            tool="opengrep",
        )
        self.assertIsInstance(resolved, paths.ResolvedPath)
        self.assertEqual(resolved.basis, paths.BASIS_SARIF_METADATA_BASE)
        self.assertIsNotNone(
            resolved.corroboration,
            msg="a fallback must record what it fell back from",
        )

    def test_a_relative_reference_with_no_base_id_needs_the_recorded_base(self) -> None:
        """The no-``uriBaseId`` branch: the metadata's base is the only anchor there is.

        Its absence is a rejection rather than a default to the root. A resolver defaulting
        to the root would produce identical rows here for a wholly different reason, and no
        row would show which had happened.
        """
        document = authored_document([authored_result(DISK_STORE_PATH)])
        rows, rejections, _counters, _tally = self.adapt(
            document, tool="opengrep", tool_base=self.explicit_base("opengrep")
        )
        self.assertEqual(rejections, [])
        self.assertEqual(rows[0]["path"], DISK_STORE_PATH)

        rows, rejections, _counters, _tally = self.adapt(
            document, tool="opengrep", tool_base=self.absent_base("opengrep")
        )
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_UNRESOLVABLE_PATH,
            label="authored relative reference, no base id, no recorded base",
        )



# --------------------------------------------------------------------------------------
# Assertions 18 and 19 -- the two errata, and the non-filesystem coordinate.
# --------------------------------------------------------------------------------------


class ErrataConformanceTests(SarifAdapterTestCase):
    """``..`` is preserved; an archive member has one defined serialization."""

    def _outside_root_prefix(self) -> str:
        """The ``../`` prefix a filesystem-root-relative target takes against this root.

        Derived from the recorded root's own depth rather than written as a literal, so the
        assertion states the arithmetic instead of a value that happens to be right for one
        root.
        """
        return "../" * len(paths.split_segments(self.scan_root))

    def test_a_location_outside_the_root_keeps_its_dot_dot_segments(self) -> None:
        """Assertion 18: ``..`` is not normalized away, and the row is still relative.

        The errata amend section 3.10.2 so that a consumer **must not** normalize ``..``
        segments out of a path. Two shapes are asserted -- an absolute ``file:`` URI outside
        the root, and a reference that already carries ``../`` -- because they reach the
        preservation through different branches of the resolver.

        The row is kept with ``in_scope`` false and counted as a non-filesystem coordinate:
        a location outside the tree is a legitimate coordinate from a correctly targeted
        runner, not evidence of a wrong scan root. Only evidence about the *runner* would
        establish that, and no such claim is made here.
        """
        elsewhere = "authored-elsewhere/module/Foo.java"
        document = authored_document(
            [
                authored_result(f"file:///{elsewhere}"),
                authored_result("../authored-sibling/Bar.scala"),
            ]
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 2)

        self.assertEqual(
            rows[0]["path"],
            f"{self._outside_root_prefix()}{elsewhere}",
            msg="an absolute location outside the root is expressed with ../ segments",
        )
        self.assertEqual(
            rows[1]["path"],
            "../authored-sibling/Bar.scala",
            msg="a reference already carrying ../ keeps it exactly",
        )
        for index, row in enumerate(rows):
            with self.subTest(row=index):
                self.assertIn("..", row["path"])
                self.assertFalse(paths.is_absolute_path(row["path"]))
                self.assertIs(
                    row["in_scope"],
                    False,
                    msg="a location outside the root is never in scope, and is kept",
                )
        self.assertEqual(
            counters[sarif.COUNTER_NON_FILESYSTEM_PATHS],
            2,
            msg="both rows are non-filesystem coordinates and are counted as such",
        )
        self.assertEqual(counters[f"{sarif.COUNTER_PATH_KIND_PREFIX}outside_root"], 2)
        self.assert_schema_invariants(rows, label="authored outside-root paths")

    def test_an_interior_dot_dot_segment_survives_inside_the_tree(self) -> None:
        """A ``..`` in the middle of an in-tree path is preserved too.

        The prohibition is on normalizing, not on ``..`` appearing at the front. A consumer
        that resolved this one would report a different path from the one the producer
        wrote, which is precisely what the amendment forbids.
        """
        reported = "core/src/main/../main/scala/org/apache/spark/storage/DiskStore.scala"
        document = authored_document([authored_result(reported)])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(
            rows[0]["path"],
            reported,
            msg="the interior .. segment must survive exactly as reported",
        )

    def test_an_archive_member_has_one_defined_serialization(self) -> None:
        """Assertion 19: ``<container-relative-to-root>!<member>``, one separator.

        Both the ``jar:`` URL form and the bare ``!`` form must produce the same string:
        the container relativized like any other path, exactly one ``!``, and the member
        with no leading slash, so there is no ambiguous ``!/`` and no second separator.
        """
        container = "core/src/main/authored-bundle.jar"
        member = "org/apache/spark/Authored.class"
        expected = f"{container}{paths.ARCHIVE_SEPARATOR}{member}"
        for uri in (f"jar:{container}!/{member}", f"{container}!/{member}"):
            with self.subTest(uri=uri):
                document = authored_document([authored_result(uri)])
                rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
                self.assertEqual(rejections, [])
                self.assertEqual(rows[0]["path"], expected)
                self.assertEqual(
                    rows[0]["path"].count(paths.ARCHIVE_SEPARATOR),
                    1,
                    msg="exactly one separator, so container and member stay unambiguous",
                )
                self.assertNotIn(
                    f"{paths.ARCHIVE_SEPARATOR}/",
                    rows[0]["path"],
                    msg="the member's leading slash is removed by the serialization",
                )
                self.assertIs(
                    rows[0]["in_scope"],
                    False,
                    msg=(
                        "an archive member is never in scope, and the rule is applied "
                        "before the globs so it cannot match core/src/main/** on its "
                        "segments alone"
                    ),
                )
                self.assertEqual(rows[0]["path"], expected)
                self.assertEqual(counters[sarif.COUNTER_NON_FILESYSTEM_PATHS], 1)
                self.assertEqual(
                    counters[f"{sarif.COUNTER_PATH_KIND_PREFIX}archive_member"], 1
                )
                self.assert_schema_invariants(rows, label=f"authored archive {uri}")

    def test_an_archive_container_outside_the_root_keeps_its_dot_dot_segments(self) -> None:
        """The two errata together: a container outside the root, relativized not normalized."""
        document = authored_document(
            [authored_result("jar:../authored-sibling/lib.jar!/org/apache/Authored.class")]
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(
            rows[0]["path"],
            "../authored-sibling/lib.jar!org/apache/Authored.class",
            msg="the container is relativized like any other path and keeps its ../",
        )
        self.assertIs(rows[0]["in_scope"], False)

    def test_a_single_leading_slash_is_handled_rather_than_rejected(self) -> None:
        """Errata issue 480: a single leading slash is not, on its own, grounds to reject.

        The amendment permits it where it is required to distinguish items in an archive
        format, so a reference beginning with one slash is read as absolute only where it
        demonstrably names a location under the recorded root, and otherwise as the
        archive-distinguishing relative form. Both readings are recorded in the basis, so
        neither is silent -- and neither is a rejection, which is the assertion.
        """
        in_archive = "/authored-bundle.jar!/org/apache/Authored.class"
        resolved = paths.resolve_sarif_location(
            in_archive,
            None,
            None,
            self.scan_root,
            self.explicit_base("opengrep"),
            tool="opengrep",
        )
        self.assertIsInstance(
            resolved,
            paths.ResolvedPath,
            msg=(
                "an in-archive reference with one leading slash is the errata-480 shape "
                "and must not be rejected on that ground alone"
            ),
        )
        self.assertEqual(resolved.kind, "archive_member")

        under_root = f"/{DISK_STORE_PATH}"
        document = authored_document([authored_result(under_root)])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(
            rows[0]["path"],
            DISK_STORE_PATH,
            msg="the slash is read as the errata form and the remainder as root-relative",
        )
        basis = paths.resolve_sarif_location(
            under_root,
            None,
            None,
            self.scan_root,
            self.explicit_base("opengrep"),
            tool="opengrep",
        ).basis
        self.assertEqual(
            basis,
            paths.BASIS_ARCHIVE_LEADING_SLASH,
            msg="the reading taken must be recorded in the basis rather than left implicit",
        )

    def test_a_nested_archive_reference_is_a_counted_rejection(self) -> None:
        """Two ``!`` separators are not describable in the defined serialization.

        Reject rather than infer: the form describes one container and one member, so a
        second separator would have to be invented. The record is counted as malformed
        rather than serialized into a shape the single-separator invariant forbids.
        """
        document = authored_document(
            [authored_result("core/outer.jar!/inner.jar!/org/apache/Authored.class")]
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_MALFORMED_RECORD,
            label="authored nested archive reference",
        )

    def test_a_non_filesystem_row_is_kept_and_counted_never_dropped(self) -> None:
        """The proportion reported per tool depends on these rows being emitted.

        An archive member and an outside-root location beside two ordinary rows: all four
        are emitted, two are counted as non-filesystem coordinates, and the reconciliation
        identity holds -- so nothing was quietly filtered on ``in_scope``.
        """
        document = authored_document(
            [
                authored_result(DISK_STORE_PATH),
                authored_result("core/src/main/authored.jar!/org/apache/A.class"),
                authored_result("../authored-sibling/B.scala"),
                authored_result(PYSPARK_TEST_MODULE_PATH),
            ]
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 4, msg="every non-filesystem row is kept")
        self.assertEqual(counters[sarif.COUNTER_NON_FILESYSTEM_PATHS], 2)
        self.assertEqual(counters[sarif.COUNTER_ROWS_IN_SCOPE], 2)
        self.assertEqual(counters[sarif.COUNTER_ROWS_OUT_OF_SCOPE], 2)
        self.assert_reconciliation_identity(
            document, rows, rejections, label="authored mixed coordinates", expected_records=4
        )
        self.assert_schema_invariants(rows, label="authored mixed coordinates")


# --------------------------------------------------------------------------------------
# The absent-path class, which AAP 0.5.4 folds into the same condition as the
# unresolvable-path fixture but which is a separate member of the closed set.
# --------------------------------------------------------------------------------------


class AbsentPathTests(SarifAdapterTestCase):
    """``path`` is not an optional field, so a record naming no location is rejected."""

    def test_the_ways_a_record_can_name_no_location_are_each_rejected(self) -> None:
        """Four shapes, one class: the record names no file, so there is no row to emit.

        ``path`` is never absent, so none of these can become a row with an empty path. They
        are separated from the malformed shapes below because they mean something different
        about the producer: a result addressed only by logical location names no file,
        whereas a ``locations`` value that is not an array is structurally wrong.
        """
        cases = {
            "no locations array": {"ruleId": "r", "message": {"text": "m"}},
            "empty locations array": {
                "ruleId": "r",
                "message": {"text": "m"},
                "locations": [],
            },
            "no physicalLocation": {
                "ruleId": "r",
                "message": {"text": "m"},
                "locations": [{"logicalLocations": [{"name": "authoredSymbol"}]}],
            },
            "artifactLocation with no uri": {
                "ruleId": "r",
                "message": {"text": "m"},
                "locations": [{"physicalLocation": {"artifactLocation": {}}}],
            },
        }
        for label, result in cases.items():
            with self.subTest(case=label):
                document = authored_document([result])
                rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
                self.assertEqual(rows, [])
                self.assert_single_rejection(
                    rejections,
                    reject_class=paths.REJECT_ABSENT_PATH,
                    label=f"authored {label}",
                )

    def test_a_structurally_wrong_location_chain_is_malformed_not_absent(self) -> None:
        """The two classes are kept apart, because they say different things.

        A ``locations`` that is not an array, and a location member that is not an object,
        are malformed records; collapsing them into ``absent_path`` would report a producer
        emitting a wrong type as one emitting no location.
        """
        cases = {
            "locations is not an array": {
                "ruleId": "r",
                "message": {"text": "m"},
                "locations": "core/src/main/scala/A.scala",
            },
            "first location is not an object": {
                "ruleId": "r",
                "message": {"text": "m"},
                "locations": ["core/src/main/scala/A.scala"],
            },
            "uri is not a string": {
                "ruleId": "r",
                "message": {"text": "m"},
                "locations": [{"physicalLocation": {"artifactLocation": {"uri": 7}}}],
            },
        }
        for label, result in cases.items():
            with self.subTest(case=label):
                document = authored_document([result])
                rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
                self.assertEqual(rows, [])
                self.assert_single_rejection(
                    rejections,
                    reject_class=paths.REJECT_MALFORMED_RECORD,
                    label=f"authored {label}",
                )

    def test_a_result_that_is_not_an_object_still_counts_as_one_record(self) -> None:
        """A malformed element contributes no row and one rejection, and is still a record.

        The independent traversal reads only the containers it needs in order to walk, so an
        element it can make no sense of still counts -- which is what keeps the identity
        balanced over a partially parsable artifact.
        """
        document = authored_document(
            ["a bare string where a result object belongs", authored_result(DISK_STORE_PATH)]
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(len(rows), 1, msg="the well-formed record still becomes a row")
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_MALFORMED_RECORD,
            label="authored bare-string result",
        )
        self.assert_reconciliation_identity(
            document, rows, rejections, label="authored bare-string result", expected_records=2
        )



# --------------------------------------------------------------------------------------
# Root independence, proved rather than restated.
# --------------------------------------------------------------------------------------


def fixture_has_a_base_map(document: Any) -> bool:
    """Return whether any run in ``document`` carries ``originalUriBaseIds``.

    This is what decides root dependence, and it is derived from the fixture rather than
    read from a label. With no base map every reference resolves through the
    runner-recorded base, so pointing that base at another root moves every path with it
    and the rows come back identical. With a base map naming an absolute directory, the
    chain reconstructs that directory whatever root the adapter is given, so the rows hold
    for the root the map names and for no other -- which is exactly what those expected
    files state as a precondition.
    """
    runs = document.get("runs") if isinstance(document, dict) else None
    if not isinstance(runs, list):
        return False
    return any(
        isinstance(run, dict)
        and isinstance(run.get(paths.SARIF_ORIGINAL_URI_BASE_IDS_KEY), dict)
        for run in runs
    )


class RootIndependenceTests(SarifAdapterTestCase):
    """Where an expected file claims root independence, the claim is demonstrated."""

    def test_the_derived_root_dependence_agrees_with_every_expectation(self) -> None:
        """Each expectation's recorded base-map presence matches the fixture's.

        The criterion this class acts on is derived from the fixture, and every expected
        file states the same fact independently. Requiring the two to agree is what keeps
        the derivation honest: a fixture recaptured with a base map would otherwise be
        re-run under a temporary root and fail for a reason that looked like a defect in
        the adapter.
        """
        for stem in (*POSITIVE_FIXTURES, *NEGATIVE_FIXTURES):
            expectation = _read_json(_expected_path(stem))
            recorded = expectation.get("resolution_context", {}).get(
                "sarif_original_uri_base_ids_present"
            )
            with self.subTest(stem=stem):
                self.assertIsInstance(
                    recorded,
                    bool,
                    msg=f"{stem}: the expectation records no base-map presence",
                )
                self.assertEqual(
                    fixture_has_a_base_map(_read_json(_fixture_path(stem))),
                    recorded,
                    msg=(
                        f"{stem}: the fixture's base-map presence and the expectation's "
                        "record of it disagree"
                    ),
                )

    def test_a_fixture_with_no_base_map_gives_identical_rows_under_a_temporary_root(self) -> None:
        """The rows move with the recorded base, which is what root independence means.

        Every input is swapped for one this test authored -- a temporary directory as the
        root, an allowlist written into it, and a runner-metadata document whose recorded
        base is that directory -- and all three are read back through the same loaders the
        normalizer uses. The rows must come back identical to the ones produced against the
        recorded root.

        This is the hermetic half of the module: it demonstrates that nothing in the
        resolution depends on the pinned tree existing, and it exercises the loaders on a
        document written here rather than only on the shipped one. The pin paths each
        fixture names are materialised as empty files inside the temporary tree, so the tree
        is a faithful stand-in rather than a bare directory.
        """
        independent = [
            stem
            for stem in (*POSITIVE_FIXTURES, *NEGATIVE_FIXTURES)
            if not fixture_has_a_base_map(_read_json(_fixture_path(stem)))
        ]
        self.assertTrue(
            independent, msg="no fixture is root-independent, so this class asserts nothing"
        )
        for stem in independent:
            tool = POSITIVE_FIXTURES.get(stem, NEGATIVE_FIXTURE_TOOL)
            document = _read_json(_fixture_path(stem))
            with self.subTest(stem=stem):
                recorded_rows, recorded_rejections, recorded_counters, _tally = self.adapt(
                    document, tool=tool, tool_base=self.explicit_base(tool)
                )
                root, allowlist, tool_base = self.hermetic_context(
                    tool,
                    explicit=True,
                    materialise=tuple(
                        row["path"] for row in recorded_rows if "!" not in row["path"]
                    ),
                )
                self.assertNotEqual(
                    root, self.scan_root, msg="the temporary root must differ from the recorded one"
                )
                rows, rejections, counters, _tally = self.adapt(
                    document,
                    tool=tool,
                    tool_base=tool_base,
                    root=root,
                    allowlist=allowlist,
                )
                self.assert_rows_match(rows, recorded_rows, label=f"{stem} under a temp root")
                self.assertEqual(
                    [rejection.reject_class for rejection in rejections],
                    [rejection.reject_class for rejection in recorded_rejections],
                    msg=f"{stem}: the rejection classes changed with the root",
                )
                self.assert_counters_match(
                    counters, recorded_counters, label=f"{stem} under a temp root"
                )
                self.assert_schema_invariants(rows, label=f"{stem} under a temp root")

    def test_the_hermetic_context_can_also_supply_the_absent_base_branch(self) -> None:
        """The temporary metadata drives both branches, through the same loader.

        The absent branch is reached by writing ``path_base.kind`` as ``none`` into a
        document and loading it, rather than by constructing a
        :class:`~normalize.paths.ToolPathBase` directly -- so the loader's own handling of
        that kind is exercised, and the branch is shown to be reachable from a real
        metadata document and not only from a hand-made object.
        """
        root, allowlist, tool_base = self.hermetic_context("opengrep", explicit=False)
        self.assertEqual(tool_base.kind, paths.PATH_BASE_KIND_NONE)
        self.assertFalse(tool_base.has_explicit_base)
        self.assertIsNone(tool_base.base_for_relative())
        document = authored_document([authored_result(DISK_STORE_PATH)])
        rows, rejections, _counters, _tally = self.adapt(
            document, tool="opengrep", tool_base=tool_base, root=root, allowlist=allowlist
        )
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_UNRESOLVABLE_PATH,
            label="temporary metadata recording no base",
        )


# --------------------------------------------------------------------------------------
# The ten negative fixtures. One test method, driven by the expected files, so that a
# condition added there cannot be silently left unasserted here.
# --------------------------------------------------------------------------------------


class NegativeFixtureTests(SarifAdapterTestCase):
    """Every rejection condition this adapter can produce, asserted by class name."""

    def _raw_records(self, expectation: dict[str, Any]) -> int:
        """Return the record count an expectation records, from either counts block."""
        for key in ("counts", "counts_shared"):
            block = expectation.get(key)
            if isinstance(block, dict) and isinstance(
                block.get("raw_finding_records"), int
            ):
                return block["raw_finding_records"]
        raise KeyError("the expectation records no raw_finding_records")

    def _assert_rejection(
        self,
        rejection: paths.Rejection,
        expected: dict[str, Any],
        *,
        label: str,
    ) -> None:
        """Assert one rejection against its recorded expectation, in full.

        The class, the detail verbatim, and the record identity including its key order. The
        detail matters as much as the class here: the four ``uriBaseId`` terminal cases and
        the plain unresolvable path all share ``unresolvable_path``, so the class alone
        cannot tell a cycle from an over-deep chain, and only the detail can.

        Two spellings of the same expectation are honoured, because both are in use: a
        canonical block names its members ``expected_detail`` and
        ``expected_record_identity``, while a branch block that restates one names them
        ``detail`` and ``record_identity``. Whichever is present is asserted; the ``tool``
        member is optional in a branch block and defaults to the document's own.
        """
        self.assertIn(
            expected["reject_class"],
            paths.REJECT_CLASSES,
            msg=f"{label}: the expectation names a class outside the closed set of ten",
        )
        self.assertEqual(
            rejection.reject_class,
            expected["reject_class"],
            msg=(
                f"{label}: rejected under {rejection.reject_class!r}, the expectation says "
                f"{expected['reject_class']!r}"
            ),
        )
        self.assertEqual(
            rejection.tool,
            expected.get("tool", NEGATIVE_FIXTURE_TOOL),
            msg=f"{label}: the rejection names the wrong tool",
        )
        detail = expected.get("expected_detail", expected.get("detail"))
        substrings = expected.get("expected_detail_substrings")
        if isinstance(detail, str):
            self.assertEqual(
                rejection.detail,
                detail,
                msg=f"{label}: the retained diagnostic differs from the recorded one",
            )
        elif isinstance(substrings, list):
            # One expectation records substrings rather than a whole string, and says why:
            # its detail carries the traversal bound's own value, so asserting the string
            # verbatim would couple this test to that constant and make raising the bound a
            # test failure rather than a change. The substrings still pin the diagnosis.
            for substring in substrings:
                self.assertIn(
                    substring,
                    rejection.detail,
                    msg=(
                        f"{label}: the detail no longer carries {substring!r}, so the "
                        "diagnosis it records is no longer the one produced"
                    ),
                )
        else:
            self.fail(
                f"{label}: the expectation records no detail under 'expected_detail', "
                "'detail' or 'expected_detail_substrings', so the sub-reason -- the only "
                "thing distinguishing this condition from its siblings under the same "
                "class -- would go unasserted"
            )
        self.assertTrue(
            rejection.detail.strip(),
            msg=f"{label}: an empty detail is the catch-all the contract forbids",
        )
        for substring in expected.get("distinguishing_substrings", ()) or ():
            if isinstance(substring, str):
                self.assertIn(
                    substring,
                    rejection.detail,
                    msg=(
                        f"{label}: the detail no longer distinguishes this condition from "
                        f"its siblings -- {substring!r} is absent"
                    ),
                )
        for substring in expected.get("expected_detail_must_not_contain", ()) or ():
            if isinstance(substring, str):
                self.assertNotIn(
                    substring,
                    rejection.detail,
                    msg=(
                        f"{label}: the detail contains {substring!r}, which its expectation "
                        "records as evidence that a gate was reached that must not be"
                    ),
                )
        identity = expected.get(
            "expected_record_identity", expected.get("record_identity")
        )
        self.assertIsInstance(
            identity,
            dict,
            msg=(
                f"{label}: the expectation records no identifying fields under either "
                "'expected_record_identity' or 'record_identity'"
            ),
        )
        self.assertEqual(
            dict(rejection.record_identity),
            identity,
            msg=f"{label}: the record identity differs from the recorded one",
        )
        self.assertEqual(
            list(rejection.record_identity),
            list(identity),
            msg=(
                f"{label}: the identity's keys are in a different order, so a reader "
                "comparing the two documents field by field would see a difference"
            ),
        )

    def _assert_outcome(
        self,
        stem: str,
        block: dict[str, Any],
        *,
        kind: str,
        label: str,
        counters: dict[str, Any] | None,
    ) -> None:
        """Assert one fixture under one metadata context against one expectation block."""
        document = _read_json(_fixture_path(stem))
        rows, rejections, produced_counters, _tally = self.adapt(
            document,
            tool=NEGATIVE_FIXTURE_TOOL,
            tool_base=self.base_of_kind(NEGATIVE_FIXTURE_TOOL, kind),
        )
        self.assert_rows_match(rows, block["rows"], label=label)
        self.assert_schema_invariants(rows, label=label)
        expected_rejections = block["rejections"]
        self.assertEqual(
            len(rejections),
            len(expected_rejections),
            msg=(
                f"{label}: {len(rejections)} rejections "
                f"({[rejection.reject_class for rejection in rejections]!r}), "
                f"{len(expected_rejections)} expected"
            ),
        )
        for index, (rejection, expected) in enumerate(zip(rejections, expected_rejections)):
            self._assert_rejection(rejection, expected, label=f"{label} rejection {index}")
        if counters is not None:
            self.assert_counters_match(produced_counters, counters, label=label)

    def test_every_negative_fixture_produces_its_recorded_rejection(self) -> None:
        """Each of the ten conditions, under the metadata context its expectation records.

        The offending record produces no row and one rejection under the class the
        expectation names, while every other record in the same fixture still becomes a row
        -- which is the rejection boundary: a partial parse emits everything parsable rather
        than abandoning the artifact.
        """
        for stem in NEGATIVE_FIXTURES:
            expectation = _read_json(_expected_path(stem))
            with self.subTest(fixture=stem):
                self.assertEqual(expectation["tool"], NEGATIVE_FIXTURE_TOOL)
                if "branches" in expectation:
                    # Handled by the two-branch test below, which asserts both outcomes.
                    continue
                self._assert_outcome(
                    stem,
                    expectation,
                    kind=expected_path_base_kind(expectation),
                    label=stem,
                    counters=expectation.get("counters"),
                )

    def test_the_two_branch_expectations_are_asserted_in_both_branches(self) -> None:
        """A fixture whose outcome the metadata decides is asserted under both contexts.

        One of the two differs between the branches -- a row under an explicit base, a
        counted rejection without one -- and the other is identical in both, because its
        walk outcome is not eligible for the fallback and the branch condition is never
        consulted. Both are asserted from the expectation's own branch blocks rather than
        assumed, so the pair that coincides is shown to coincide rather than left unexamined.
        """
        for stem in TWO_BRANCH_EXPECTATIONS:
            expectation = _read_json(_expected_path(stem))
            branches = expectation["branches"]
            self.assertEqual(
                len(branches), 2, msg=f"{stem}: expected exactly two branch blocks"
            )
            for branch in branches:
                kind = branch["precondition"]["tool_path_base"]["kind"]
                with self.subTest(fixture=stem, branch=branch["branch_id"]):
                    self._assert_outcome(
                        stem,
                        branch,
                        kind=kind,
                        label=f"{stem} branch {branch['branch_id']}",
                        counters=branch.get("counters", expectation.get("counters")),
                    )
            if "rows" in expectation:
                for branch in branches:
                    with self.subTest(fixture=stem, branch=branch["branch_id"], check="canonical"):
                        self.assertEqual(
                            branch["rows"],
                            expectation["rows"],
                            msg=(
                                f"{stem}: the branch's rows and the canonical rows have "
                                "drifted apart inside one document"
                            ),
                        )

    def test_the_reconciliation_identity_holds_on_every_negative_fixture(self) -> None:
        """``records walked == rows + rejections``, with the rejected records counted.

        The left side comes from the traversal that builds nothing, so the identity is a
        check rather than a tautology, and a fixture whose defective element cannot be
        parsed at all still contributes one record to it.
        """
        for stem in NEGATIVE_FIXTURES:
            expectation = _read_json(_expected_path(stem))
            document = _read_json(_fixture_path(stem))
            contexts: list[tuple[str, str]] = []
            if "branches" in expectation:
                contexts = [
                    (branch["branch_id"], branch["precondition"]["tool_path_base"]["kind"])
                    for branch in expectation["branches"]
                ]
            else:
                contexts = [("canonical", expected_path_base_kind(expectation))]
            for branch_id, kind in contexts:
                with self.subTest(fixture=stem, branch=branch_id):
                    rows, rejections, _counters, _tally = self.adapt(
                        document,
                        tool=NEGATIVE_FIXTURE_TOOL,
                        tool_base=self.base_of_kind(NEGATIVE_FIXTURE_TOOL, kind),
                    )
                    self.assert_reconciliation_identity(
                        document,
                        rows,
                        rejections,
                        label=f"{stem} ({branch_id})",
                        expected_records=self._raw_records(expectation),
                    )

    def test_a_rejected_record_contributes_no_severity_literal(self) -> None:
        """The tally is fed once per emitted row, never once per record.

        A literal counted for a rejected record would put it in ``severity-map.md`` against
        rows the dataset does not contain, and the per-literal counts there are row counts.
        """
        for stem in NEGATIVE_FIXTURES:
            expectation = _read_json(_expected_path(stem))
            if "branches" in expectation:
                kind = expectation["branches"][0]["precondition"]["tool_path_base"]["kind"]
            else:
                kind = expected_path_base_kind(expectation)
            with self.subTest(fixture=stem):
                rows, _rejections, _counters, tally = self.adapt_fixture(
                    stem,
                    tool=NEGATIVE_FIXTURE_TOOL,
                    tool_base=self.base_of_kind(NEGATIVE_FIXTURE_TOOL, kind),
                )
                self.assertEqual(
                    len(tally.results),
                    len(rows),
                    msg="the tally must be fed exactly once per emitted row",
                )
                self.assertEqual(
                    tally.delegate.row_count(NEGATIVE_FIXTURE_TOOL),
                    len(rows),
                    msg="the real tally's row count must agree with the rows emitted",
                )

    def test_every_producible_condition_has_a_fixture_that_produces_it(self) -> None:
        """The seven classes this adapter can produce are each actually produced.

        The claim the module docstring makes, checked against what the fixtures and the
        authored documents in this file actually yield rather than argued. A condition listed
        as producible but never produced would be a condition nobody has exercised, which is
        the state AAP 0.6.2 exists to prevent.
        """
        produced: set[str] = set()
        for stem in NEGATIVE_FIXTURES:
            expectation = _read_json(_expected_path(stem))
            if "branches" in expectation:
                contexts = [
                    branch["precondition"]["tool_path_base"]["kind"]
                    for branch in expectation["branches"]
                ]
            else:
                contexts = [expected_path_base_kind(expectation)]
            for kind in contexts:
                _rows, rejections, _counters, _tally = self.adapt_fixture(
                    stem,
                    tool=NEGATIVE_FIXTURE_TOOL,
                    tool_base=self.base_of_kind(NEGATIVE_FIXTURE_TOOL, kind),
                )
                produced.update(rejection.reject_class for rejection in rejections)
        for name in (
            paths.REJECT_UNRESOLVABLE_PATH,
            paths.REJECT_INVALID_URI,
            paths.REJECT_MISSING_RULE_ID,
            paths.REJECT_MISSING_MESSAGE,
            paths.REJECT_NON_INTEGER_START_LINE,
            paths.REJECT_MALFORMED_RECORD,
        ):
            with self.subTest(reject_class=name):
                self.assertIn(
                    name,
                    produced,
                    msg=(
                        f"no captured negative fixture produces {name!r}; the condition is "
                        "listed as producible and is not exercised by a fixture"
                    ),
                )



# --------------------------------------------------------------------------------------
# The adapter's own argument contract. A caller fault is raised rather than absorbed into
# a rejection count, and each of these would otherwise produce a plausible dataset.
# --------------------------------------------------------------------------------------


class AdapterContractTests(SarifAdapterTestCase):
    """Every argument the adapter refuses, refused for the reason it documents."""

    def _valid_kwargs(self) -> dict[str, Any]:
        """The arguments a well-formed call carries, so one at a time can be spoiled."""
        return {
            "tool": "opengrep",
            "root": self.scan_root,
            "tool_base": self.explicit_base("opengrep"),
            "allowlist": self.allowlist,
            "tally": RecordingTally(),
        }

    def _expect_refusal(self, document: Any, **overrides: Any) -> None:
        """Assert the adapter raises rather than returning a plausible empty result."""
        kwargs = self._valid_kwargs()
        kwargs.update(overrides)
        with self.assertRaises(sarif.SarifAdapterError):
            sarif.adapt(document, **kwargs)

    def test_a_well_formed_call_is_accepted(self) -> None:
        """The control: without it, every refusal below could be refusing for another reason."""
        rows, rejections, _counters, _tally = self.adapt(
            authored_document([authored_result(DISK_STORE_PATH)]), tool="opengrep"
        )
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 1)

    def test_a_tool_this_adapter_does_not_serve_is_refused(self) -> None:
        """One module serves three tools, so the identifier is required rather than inferred.

        ``joern`` is the instructive case: it is ``sast`` like the three, and its artifact is
        native rather than SARIF, so it has its own adapter. Passing it here would stamp its
        identifier onto rows this adapter built from a different shape.
        """
        document = authored_document([authored_result(DISK_STORE_PATH)])
        for tool in ("joern", "trivy", "Opengrep", ""):
            with self.subTest(tool=tool):
                self._expect_refusal(document, tool=tool)

    def test_a_relative_or_non_text_root_is_refused(self) -> None:
        """A relative root cannot anchor anything, so it is refused on the call.

        Accepting one would produce a plausible-looking wrong answer for every row rather
        than an error on any of them.
        """
        document = authored_document([authored_result(DISK_STORE_PATH)])
        for root in ("relative/root", "", b"/opt/bytes-root"):
            with self.subTest(root=root):
                self._expect_refusal(document, root=root)

    def test_another_tools_path_base_is_refused(self) -> None:
        """Resolving one tool's paths against another's recorded base is refused.

        It would resolve every path against the wrong base while every row still looked
        well-formed, which is exactly the failure "every base taken from the recorded runner
        metadata" exists to prevent.
        """
        document = authored_document([authored_result(DISK_STORE_PATH)])
        self._expect_refusal(document, tool_base=self.explicit_base("semgrep"))
        self._expect_refusal(document, tool_base=None)
        self._expect_refusal(document, tool_base=self.scan_root)

    def test_an_allowlist_that_is_a_string_or_not_iterable_is_refused(self) -> None:
        """A single string would be iterated character by character, so it is refused.

        Every row would then take ``in_scope`` false, which is indistinguishable from a
        scope that legitimately covers nothing.
        """
        document = authored_document([authored_result(DISK_STORE_PATH)])
        self._expect_refusal(document, allowlist="core/src/main/**")
        self._expect_refusal(document, allowlist=None)
        self._expect_refusal(document, allowlist=("core/src/main/**", ""))

    def test_a_tally_that_cannot_record_is_refused(self) -> None:
        """Every row's literal has to reach ``severity-map.md``, so a silent tally is refused.

        A capability check rather than a class check, which is what lets this module's
        order-recording double be used at all -- and ``None`` is still refused, because a
        skipped tally would leave that document under-reporting with nothing to show it had.
        """
        document = authored_document([authored_result(DISK_STORE_PATH)])
        self._expect_refusal(document, tally=None)
        self._expect_refusal(document, tally=object())

    def test_a_document_that_is_not_a_sarif_envelope_is_refused_rather_than_emptied(self) -> None:
        """The failure this refusal prevents is silent, which is why it is a refusal.

        A permissive adapter handed a document with no ``runs`` array would look for results,
        find none, and report success with zero rows -- and an empty result set is
        indistinguishable from a clean scan. The documents below are authored here rather
        than borrowed from another adapter's fixture: shape *routing* is owned by the
        mandated shape-routing negative test, and what is asserted here is this adapter's own
        refusal once something reaches it.
        """
        for label, document in (
            ("a top-level array, as a native shape would be", [{"RuleID": "authored"}]),
            ("a top-level scalar", "not a document"),
            ("an object with no runs member", {"version": "2.1.0"}),
            ("a runs member that is not an array", {"version": "2.1.0", "runs": {}}),
            ("None", None),
        ):
            with self.subTest(document=label):
                self._expect_refusal(document)

    def test_an_empty_but_conformant_envelope_yields_no_rows_and_no_error(self) -> None:
        """A run with an empty results array is the ordinary shape of a clean scan.

        The other side of the refusal above: an empty *result set* from a conformant
        envelope is legitimate and must not raise, so the refusal cannot be implemented by
        rejecting emptiness.
        """
        rows, rejections, counters, _tally = self.adapt(
            authored_document([]), tool="opengrep"
        )
        self.assertEqual(rows, [])
        self.assertEqual(rejections, [])
        self.assertEqual(counters[sarif.COUNTER_RUNS], 1)
        self.assert_reconciliation_identity(
            authored_document([]), rows, rejections, label="empty run", expected_records=0
        )

    def test_a_run_with_no_results_array_is_counted_rather_than_failed(self) -> None:
        """A run carrying no ``results`` contributes nothing, and the fact is counted.

        Counted rather than passed over in silence, so that the adapter and the independent
        traversal agreeing on zero is visible rather than merely assumed.
        """
        document = {"version": "2.1.0", "runs": [{"tool": {"driver": {"name": "d"}}}]}
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        self.assertEqual(rejections, [])
        self.assertEqual(counters[sarif.COUNTER_RUNS_WITHOUT_RESULTS_ARRAY], 1)
        self.assert_reconciliation_identity(
            document, rows, rejections, label="run with no results", expected_records=0
        )


# --------------------------------------------------------------------------------------
# This module's own hygiene. Three of the Phase 9 validation items are properties of this
# file rather than of the adapter, and a property nobody checks is a property nobody has.
# --------------------------------------------------------------------------------------


class ModuleHygieneTests(SarifAdapterTestCase):
    """The file's own contract: stdlib only, twelve fields, and no secret in the source."""

    def test_this_module_imports_only_the_standard_library_and_normalize(self) -> None:
        """No third-party module is imported, asserted from this file's own syntax tree.

        AAP 0.4.1 fixes this tree to the CPython standard library and AAP 0.4.3 adds no
        dependency in any direction, so a ``pytest`` import here would be a defect rather
        than a convenience. The check is on the source rather than on ``sys.modules``,
        because discovery imports sibling modules into the same process and a shared
        ``sys.modules`` cannot attribute an import to a file.

        ``normalize.reconcile`` is excluded deliberately as well as incidentally: it is not
        among this file's declared dependencies, and ``test_reconciliation.py`` owns it.
        """
        permitted_stdlib = {
            "ast",
            "hashlib",
            "json",
            "sys",
            "tempfile",
            "unittest",
            "pathlib",
            "typing",
            "__future__",
        }
        permitted_internal = {
            "normalize",
            "normalize.adapters",
        }
        tree = ast.parse(_THIS_FILE.read_text(encoding="utf-8"))
        seen: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                seen.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                seen.add(node.module)
        for name in sorted(seen):
            with self.subTest(module=name):
                self.assertTrue(
                    name in permitted_stdlib
                    or name in permitted_internal
                    or name.split(".")[0] in permitted_stdlib,
                    msg=(
                        f"{name!r} is neither one of the standard-library modules this file "
                        "declares nor a normalize module it depends on"
                    ),
                )
        self.assertIn("normalize", seen, msg="the modules under test must be imported")
        self.assertNotIn(
            "normalize.reconcile",
            seen,
            msg="reconcile.py is not a declared dependency of this file",
        )

    def test_the_schema_is_the_twelve_fields_in_the_requested_order(self) -> None:
        """``emit.FIELDS`` is the contract, and this states it once, explicitly.

        Everything else in this module iterates the constant rather than a literal, which
        is what keeps the field order asserted from the authored source. That leaves one
        thing unstated: what the constant itself must be. This is it.
        """
        self.assertEqual(
            emit.FIELDS,
            (
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
            ),
        )
        self.assertEqual(len(emit.FIELDS), 12)
        self.assertEqual(len(set(emit.FIELDS)), 12, msg="no field may be repeated")
        self.assertEqual(sarif.FIELDS, emit.FIELDS, msg="the adapter and the emitter agree")

    def test_no_secret_shaped_literal_appears_in_this_file(self) -> None:
        """This tree is committed to git, so the source is checked for credential shapes.

        The tokens searched for are assembled from fragments rather than written out, so
        this test can look for a provider prefix without the file containing one -- and so
        the check itself cannot become the thing it is checking for. No adapter field is
        populated from a secret either: Gitleaks runs with redaction and its adapter is a
        different module's subject, and nothing here carries a matched value into a row.
        """
        source = _THIS_FILE.read_text(encoding="utf-8")
        # Each token is split so that the assembled string never appears in this file.
        markers = (
            "sk" + "_live_",
            "sk" + "_test_",
            "pk" + "_live_",
            "AK" + "IA",
            "AS" + "IA",
            "gh" + "p_",
            "gh" + "o_",
            "gh" + "s_",
            "github" + "_pat_",
            "xo" + "xb-",
            "xo" + "xp-",
            "AI" + "za",
            "BEGIN " + "PRIVATE KEY",
            "BEGIN RSA " + "PRIVATE KEY",
            "ey" + "J",
        )
        for marker in markers:
            with self.subTest(marker=marker):
                self.assertNotIn(
                    marker,
                    source,
                    msg=f"a literal shaped like a credential ({marker!r}) is in this file",
                )

    def test_the_bootstrap_is_idempotent_and_derived_from_this_file(self) -> None:
        """The library directory appears once on ``sys.path``, and comes from ``__file__``.

        Discovery imports sibling test modules that perform the same insertion, so a
        bootstrap without its membership guard would leave duplicate entries behind for the
        rest of the process. Deriving the entry from ``__file__`` is what makes the suite
        run identically from any working directory.
        """
        self.assertEqual(
            sys.path.count(_LIB_DIR),
            1,
            msg="the library directory must appear exactly once on sys.path",
        )
        self.assertEqual(REPO_ROOT, _THIS_FILE.parents[2])
        self.assertTrue((REPO_ROOT / "harness" / "lib" / "normalize").is_dir())
        self.assertFalse(
            (REPO_ROOT / "harness" / "lib" / "normalize" / "__init__.py").exists(),
            msg=(
                "the package is an implicit namespace package, which is why the bootstrap "
                "puts harness/lib on sys.path rather than importing an installed module"
            ),
        )

    def test_nothing_under_the_normalize_package_is_written_by_this_module(self) -> None:
        """The modules under test are read, never edited from here.

        A defect this file reveals in ``harness/lib/normalize/`` is reported, not repaired,
        so the digests of the five modules it depends on are required unchanged across the
        run. The fixtures are covered by :meth:`SarifAdapterTestCase.tearDown`; this covers
        the code.
        """
        package = REPO_ROOT / "harness" / "lib" / "normalize"
        subjects = (
            package / "paths.py",
            package / "severity.py",
            package / "emit.py",
            package / "cli.py",
            package / "adapters" / "sarif.py",
        )
        before = {path: _sha256(path) for path in subjects}
        self.adapt(authored_document([authored_result(DISK_STORE_PATH)]), tool="opengrep")
        for path, digest in before.items():
            with self.subTest(module=path.name):
                self.assertEqual(
                    _sha256(path),
                    digest,
                    msg=f"{path} changed during the run; nothing here may write to it",
                )


if __name__ == "__main__":  # pragma: no cover - convenience for a direct invocation
    # Discovery is the documented route (``python3 -m unittest discover ...``); this makes
    # ``python3 oss-scan-results/adapter-tests/test_sarif_adapter.py`` work as well, which
    # is useful while narrowing a single failure. Neither route needs a plugin.
    unittest.main(verbosity=2)

