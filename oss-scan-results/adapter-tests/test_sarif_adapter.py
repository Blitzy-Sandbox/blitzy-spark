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

The twenty-two required assertions, the two provenance assertions, and their owners
-----------------------------------------------------------------------------------
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
23. every captured positive fixture *is* that
    tool's raw artifact -- record for record,
    rule for rule, envelope member for
    envelope member ............................. :class:`RawArtifactProvenanceTests`
24. every derived fixture declares itself
    derived and is measurably not a raw
    excerpt ..................................... :class:`RawArtifactProvenanceTests`

Assertions 23 and 24 are not in AAP 0.6.1's list of what this file covers; they are what
makes the rest of it mean anything. AAP 0.6.2 defines a positive fixture as *"an
unmodified captured excerpt"* of the tool's own output and gives the reason -- a
hand-written fixture tests the adapter against the shape someone believed the tool emits
-- and they are the only assertions here that open ``harness/artifacts/raw/`` at all. A
fixture's sha256, recorded in the expected file that fixture owns, can only show the file
has not changed since that digest was taken; it cannot show where the bytes came from.
The captures and the derived fixtures are measured by one shared check
(:meth:`RawArtifactProvenanceTests.provenance_defects`), which the captures must pass with
no defect and the derived fixtures must fail with at least one, so neither category can be
quietly judged by the other's standard.

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
and never written: :meth:`SarifAdapterTestCase.setUpClass` digests every committed
fixture and :meth:`SarifAdapterTestCase.tearDown` re-checks it, so a test that mutated one
would fail rather than pass quietly, and
:meth:`FixtureInventoryTests.test_every_expectation_records_its_fixtures_byte_size_and_digest`
requires each expected file's recorded size and sha256 to be those of the file on disk, so
an expectation cannot describe one file while the assertions run against another. Both are
mutation tripwires and neither is provenance evidence; assertions 23 and 24 are. Nothing under ``harness/lib/normalize/`` is edited from
here; a defect this file reveals there is reported, not repaired.

No user-specified rule governs this file; enterprise-standard best practice applies in
its place (AAP 0.7, AAP 0.10.2). That absence is expressly not licence to lower the bar
-- concretely: every one of the twelve fields is asserted individually against a
hand-verified value rather than by a single whole-dict comparison, every rejection class
is asserted by name, and no assertion is softened to make a test pass.

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

#: The runners' own verbatim artifacts -- the provenance authority for every captured
#: fixture (AAP 0.6.2: a positive fixture is *"an unmodified captured excerpt"* of the
#: tool's own output). :class:`RawArtifactProvenanceTests` opens the artifact for each
#: captured fixture and compares record for record, because a digest a fixture owns can
#: only prove the fixture is self-consistent -- it cannot prove where the bytes came
#: from. These files are read and never written; nothing under ``harness/artifacts/``
#: is created, cleared or edited from a test.
RAW_DIR = REPO_ROOT / "harness" / "artifacts" / "raw"

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

#: Derived fixture stem -> the canonical tool identifier whose shape it carries.
#:
#: These are **not** captures and never claim to be. Each is an authored document in that
#: tool's SARIF shape, carrying the feature cases its expected file enumerates, and each
#: declares itself derived in that expectation's ``fixture.provenance`` block rather than
#: leaving provenance to be inferred -- so no fixture claims captured provenance it does
#: not have. One file cannot satisfy both AAP requirements at once: AAP 0.6.2 requires a
#: positive fixture to be an unmodified captured excerpt, and AAP 0.9.4 requires every
#: behaviour to keep its coverage. A capture plus a derived companion under a
#: ``derived-`` name satisfies both, and the ``derived-`` prefix is what makes the two
#: categories distinguishable from the inventory alone.
#:
#: Each one exists because the case it carries is measurably unreachable from captured
#: output in this provisioning: no raw SARIF artifact here emits
#: ``run.originalUriBaseIds``, none carries a result with a ``ruleIndex`` and no
#: ``ruleId``, none carries a rule listing more than one CVE identifier, and no raw
#: ``datadog-static-analyzer`` rule carries ``properties.severity``,
#: ``properties.problem.severity`` or a ``defaultConfiguration``. The measurements are
#: recorded in each derived expectation.
DERIVED_FIXTURES: dict[str, str] = {
    "derived-semgrep-features": "semgrep",
    "derived-datadog-static-analyzer-features": "datadog-static-analyzer",
}

#: Every committed fixture that produces rows, captured or derived, with its tool.
#:
#: The generic row, derivation, schema and root-independence loops iterate this rather
#: than :data:`POSITIVE_FIXTURES`, so moving a case out of a capture and into a derived
#: fixture re-points the assertion instead of dropping it.
#: :data:`POSITIVE_FIXTURES` stays exactly ``sarif.SUPPORTED_TOOLS`` and is what
#: :class:`RawArtifactProvenanceTests` holds to the captured-excerpt contract.
ROW_FIXTURES: dict[str, str] = {**POSITIVE_FIXTURES, **DERIVED_FIXTURES}

#: The negative fixtures, one per rejection condition this adapter can produce, plus the
#: four that separate the ways a record can state no usable location or line: no
#: ``locations`` array at all, and a ``startLine`` of zero, of a negative value, or of a
#: boolean, and one that separates the two ways a record can state an identifier the
#: adapter cannot use -- stating none at all, and stating two that disagree.
#: Three stems separate the three ways a reference can fail to be a URI, and they are
#: three moments rather than one condition written three times.
#: ``reject-sarif-uribaseid-invalid-uri`` carries a base entry whose ``uri`` is invalid as
#: written, refused by the parser it is handed to.
#: ``reject-sarif-percent-encoded-control`` carries references that are valid as written
#: and invalid once percent-decoded, because a control-character check made only before
#: decoding is not the guard it appears to be -- ``%1b`` is three ordinary URI characters
#: until ``unquote`` turns it into ESC (CWE-176 for the decode, CWE-117 for where the
#: decoded value arrives).
#: ``reject-sarif-malformed-percent-escape`` carries references whose ``%`` is not the
#: start of an escape at all, refused BEFORE any decode, because
#: :func:`urllib.parse.unquote` leaves a malformed escape in place rather than raising --
#: so ``%``, ``%2`` and ``%GG`` decode to themselves, pass every downstream test and
#: reach the ``path`` column as text that is not a path (SEC-06). All three land on AAP
#: 0.5.4's ``unresolvable_path``/``invalid_uri`` boundary.
#: All but one are ``opengrep`` artifacts: the condition under test is usually a property
#: of the shared adapter, and one producer's shape is enough to exercise it.
#: No comparison between producers is implied or made.
NEGATIVE_FIXTURES: tuple[str, ...] = (
    "reject-sarif-unresolvable-path",
    "reject-sarif-uribaseid-missing-base",
    "reject-sarif-uribaseid-cycle",
    "reject-sarif-uribaseid-overdepth",
    "reject-sarif-uribaseid-invalid-uri",
    "reject-sarif-percent-encoded-control",
    "reject-sarif-malformed-percent-escape",
    "reject-sarif-uribaseid-relative-no-absolute-ancestor",
    "reject-sarif-missing-rule-id",
    "reject-sarif-rule-index-mismatch",
    "reject-sarif-missing-message",
    "reject-sarif-non-integer-start-line",
    "reject-sarif-malformed-record",
    "reject-sarif-absent-path",
    "reject-sarif-zero-start-line",
    "reject-sarif-negative-start-line",
    "reject-sarif-boolean-start-line",
)

#: The tool most negative fixtures were derived from, and the default for any stem
#: :data:`NEGATIVE_FIXTURE_TOOLS` does not name.
NEGATIVE_FIXTURE_TOOL = "opengrep"

#: The negative fixtures whose producer is *not* :data:`NEGATIVE_FIXTURE_TOOL`, with the
#: canonical tool identifier each one carries.
#:
#: One entry, and its tool is forced by the shape under test rather than preferred.
#: ``reject-sarif-rule-index-mismatch`` exercises a result whose ``ruleId`` and
#: ``ruleIndex`` name different rules, which needs a producer that emits both descriptors
#: on one result. Measured over ``harness/artifacts/raw/``, all 6832
#: ``datadog-static-analyzer`` results carry both, while every one of ``opengrep``'s 1322
#: and ``semgrep``'s 1162 carries ``ruleId`` alone -- so an ``opengrep``-shaped fixture
#: for this condition would be an authored shape claiming to be a derived one. The tool
#: each expectation records is asserted against this map, so a fixture cannot be adapted
#: under a tool its own expected file does not name.
NEGATIVE_FIXTURE_TOOLS: dict[str, str] = {
    "reject-sarif-rule-index-mismatch": "datadog-static-analyzer",
}


def negative_fixture_tool(stem: str) -> str:
    """Return the canonical tool identifier a negative fixture is adapted under.

    :data:`NEGATIVE_FIXTURE_TOOLS` where it names ``stem``, and
    :data:`NEGATIVE_FIXTURE_TOOL` otherwise. Every loop over
    :data:`NEGATIVE_FIXTURES` goes through this rather than naming the default directly,
    so adding a fixture from another producer re-points every assertion at once instead of
    leaving some of them adapting it under the wrong tool -- which would not fail loudly:
    the shared adapter would still produce rows, and only the ``tool`` field of each row
    and each rejection would be wrong.
    """
    return NEGATIVE_FIXTURE_TOOLS.get(stem, NEGATIVE_FIXTURE_TOOL)

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
# Ten of the twenty-two assertions concern behaviour no captured artifact exercises, and
# each captured expected file lists them under
# ``behaviours_not_exercised_by_this_fixture`` with the fixture or the test method that
# does carry the case -- a named one in every entry, since a "cover with a derived
# fixture" that names nothing is a coverage gap written down rather than closed. They are authored in memory here rather than written to
# ``fixtures/`` wherever the assertion turns on a shape rather than on committed content:
# an authored document is not captured output, and an unnamed one sitting in that
# directory would blur the distinction AAP 0.6.2 draws. Where a case does need a
# committed file -- because it must be exercised through the same load-and-adapt path as a
# capture -- the file is named ``derived-<tool>-features.sarif``, is listed in
# :data:`DERIVED_FIXTURES` rather than :data:`POSITIVE_FIXTURES`, and declares its
# provenance in its own expected file, which
# :class:`RawArtifactProvenanceTests` then holds it to. Every authored document is a
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
    property of each test rather than a convention. That digest is a mutation tripwire
    **within** the run and nothing more: it compares a file with itself as it stood a
    moment earlier, so it cannot establish where the bytes came from.
    :class:`RawArtifactProvenanceTests` is what establishes provenance, by opening the
    runner's own artifact under ``harness/artifacts/raw/`` and comparing record for
    record.
    """

    #: The shipped runner metadata, read once per class.
    runner_metadata: Any
    #: The scan root that document records -- ``SPARK_SRC``, read as input.
    scan_root: str
    #: The twelve globs, in file order, as the loader returns them.
    allowlist: tuple[str, ...]
    #: Fixture stem -> sha256 at class setup, re-checked after every test. Covers every
    #: committed fixture -- the three captures, the two derived features fixtures and the
    #: ten negatives -- so no committed fixture can be mutated by a test unnoticed.
    fixture_digests: dict[str, str]

    @classmethod
    def setUpClass(cls) -> None:
        """Read the three shared inputs, and digest every committed fixture."""
        super().setUpClass()
        cls.runner_metadata = paths.load_runner_metadata(RUNNER_METADATA_PATH)
        cls.scan_root = paths.metadata_scan_root(cls.runner_metadata)
        cls.allowlist = paths.load_allowlist(ALLOWLIST_PATH)
        cls.fixture_digests = {
            stem: _sha256(_fixture_path(stem))
            for stem in (*ROW_FIXTURES, *NEGATIVE_FIXTURES)
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
        """Every committed fixture and its expected file are present and valid JSON.

        The inventory is the three captures, the two derived fixtures and the fourteen
        negatives. It is iterated from the manifests rather than counted here, so adding a
        fixture to a manifest without committing the file fails loudly.
        """
        for stem in (*ROW_FIXTURES, *NEGATIVE_FIXTURES):
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

    def test_every_committed_sarif_fixture_is_claimed_by_exactly_one_manifest(self) -> None:
        """No SARIF fixture in the directory is left without an assertion.

        The direction that matters: a fixture added to ``fixtures/`` and not added to
        :data:`POSITIVE_FIXTURES`, :data:`DERIVED_FIXTURES` or :data:`NEGATIVE_FIXTURES`
        would sit there untested, and nothing else would notice. Enumerating the directory
        is what closes that.

        Deliberately count-agnostic: asserting a fixed inventory size would turn *adding*
        coverage into a failure and invite the wrong repair -- deleting the new fixture.
        What must hold is that the directory and the manifests describe the same set, and
        that the three manifests are pairwise disjoint: a stem claimed as both a capture
        and a derived fixture would let the captured-excerpt contract be asserted against
        authored material.
        """
        on_disk = {
            path.name[: -len(FIXTURE_SUFFIX)]
            for path in FIXTURES_DIR.iterdir()
            if path.is_file() and path.name.endswith(FIXTURE_SUFFIX)
        }
        asserted = set(POSITIVE_FIXTURES) | set(DERIVED_FIXTURES) | set(NEGATIVE_FIXTURES)
        self.assertEqual(
            on_disk,
            asserted,
            msg=(
                "the SARIF fixtures on disk and the ones this module asserts on differ; "
                f"untested: {sorted(on_disk - asserted)!r}, missing: "
                f"{sorted(asserted - on_disk)!r}"
            ),
        )
        for left_name, left, right_name, right in (
            ("POSITIVE_FIXTURES", set(POSITIVE_FIXTURES),
             "DERIVED_FIXTURES", set(DERIVED_FIXTURES)),
            ("POSITIVE_FIXTURES", set(POSITIVE_FIXTURES),
             "NEGATIVE_FIXTURES", set(NEGATIVE_FIXTURES)),
            ("DERIVED_FIXTURES", set(DERIVED_FIXTURES),
             "NEGATIVE_FIXTURES", set(NEGATIVE_FIXTURES)),
        ):
            with self.subTest(manifests=f"{left_name} vs {right_name}"):
                self.assertEqual(
                    left & right,
                    set(),
                    msg=(
                        f"{left_name} and {right_name} both claim "
                        f"{sorted(left & right)!r}; a stem belongs to exactly one manifest"
                    ),
                )
        self.assertEqual(
            set(ROW_FIXTURES),
            set(POSITIVE_FIXTURES) | set(DERIVED_FIXTURES),
            msg="ROW_FIXTURES is not the union of the captured and derived manifests",
        )
        for stem, tool in DERIVED_FIXTURES.items():
            with self.subTest(stem=stem):
                self.assertTrue(
                    stem.startswith("derived-"),
                    msg=(
                        f"{stem!r} is claimed as derived but is not named derived-*; the "
                        "name is the first thing a reader sees and it must not read as a "
                        "capture"
                    ),
                )
                self.assertIn(
                    tool,
                    SARIF_PRODUCERS,
                    msg=f"{stem}: {tool!r} is not a tool this adapter serves",
                )

    def test_every_uncovered_behaviour_names_something_that_exists(self) -> None:
        """Each ``cover_with`` entry names a fixture or a test that is actually there.

        Every row-producing expectation lists the behaviours its own fixture does not
        exercise, and says where each is covered instead. An entry naming nothing that
        exists -- *"a derived fixture carrying a second location on a result"* -- is a
        coverage gap written down rather than closed, and reads as coverage to anyone
        skimming the file. This resolves each entry: it must name a committed SARIF fixture
        that is on disk, a test class or test method defined in this module, or an expected
        file in ``expected/``.

        The module's own names are collected from its syntax tree rather than from
        ``dir()``, so a name that appears only inside a string cannot satisfy the check by
        accident.
        """
        tree = ast.parse(_THIS_FILE.read_text(encoding="utf-8"))
        defined = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for stem in ROW_FIXTURES:
            expectation = _read_json(_expected_path(stem))
            behaviours = expectation.get("behaviours_not_exercised_by_this_fixture")
            with self.subTest(stem=stem):
                self.assertIsInstance(
                    behaviours,
                    list,
                    msg=f"{stem}: the expectation lists no uncovered behaviours",
                )
                self.assertTrue(
                    behaviours, msg=f"{stem}: the uncovered-behaviour list is empty"
                )
            for entry in behaviours:
                cover_with = entry.get("cover_with", "")
                tokens = [
                    token.strip("`'\".,;()[]{}")
                    for token in str(cover_with).replace(",", " ").split()
                ]
                named = []
                for token in tokens:
                    if token in defined:
                        named.append(f"{token} in this module")
                    elif token.endswith(FIXTURE_SUFFIX):
                        candidate = FIXTURES_DIR / Path(token).name
                        if candidate.is_file():
                            named.append(f"fixture {candidate.name}")
                    elif token.endswith(EXPECTED_SUFFIX):
                        candidate = EXPECTED_DIR / Path(token).name
                        if candidate.is_file():
                            named.append(f"expectation {candidate.name}")
                    elif "." in token:
                        # A dotted reference such as ClassName.test_method_name.
                        head, _, tail = token.rpartition(".")
                        if tail in defined and head.rpartition(".")[2] in defined:
                            named.append(f"{token} in this module")
                with self.subTest(stem=stem, behaviour=entry.get("behaviour")):
                    self.assertTrue(
                        named,
                        msg=(
                            f"{stem}: the entry for {entry.get('behaviour')!r} says to "
                            f"cover it with {cover_with!r}, which names no fixture on "
                            "disk, no test in this module and no expected file. An "
                            "unnamed target is a gap recorded rather than closed"
                        ),
                    )

    def test_every_expectation_records_its_fixtures_byte_size_and_digest(self) -> None:
        """Each expected file's ``fixture`` block describes the file on disk.

        The module's own contract is that a fixture is loaded, used and left
        byte-identical, and each expected file states the size and sha256 it was
        hand-derived against. Both values are asserted here against the bytes on disk,
        because an expectation and the fixture it describes can otherwise drift apart: a
        replaced fixture whose expectation is not re-derived reads as a passing assertion
        over a document nobody hand-verified.

        The digest proves self-consistency and nothing more.
        :class:`RawArtifactProvenanceTests` is what proves provenance, and each captured
        expectation says so in ``fixture.excerpt_verification``.
        """
        for stem in (*ROW_FIXTURES, *NEGATIVE_FIXTURES):
            expectation = _read_json(_expected_path(stem))
            block = expectation.get("fixture")
            fixture = _fixture_path(stem)
            with self.subTest(stem=stem):
                self.assertIsInstance(
                    block,
                    dict,
                    msg=f"{stem}: the expectation carries no fixture block",
                )
                self.assertEqual(
                    block.get("path"),
                    f"oss-scan-results/adapter-tests/fixtures/{stem}{FIXTURE_SUFFIX}",
                    msg=f"{stem}: the fixture block names a different path",
                )
                self.assertEqual(
                    block.get("bytes"),
                    fixture.stat().st_size,
                    msg=(
                        f"{stem}: the expectation records {block.get('bytes')!r} bytes and "
                        f"the file is {fixture.stat().st_size}"
                    ),
                )
                self.assertEqual(
                    block.get("sha256"),
                    _sha256(fixture),
                    msg=(
                        f"{stem}: the expectation's recorded sha256 is not the digest of "
                        "the fixture on disk, so the two describe different bytes"
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
            tool = negative_fixture_tool(stem)
            _rows, rejections, _counters, _tally = self.adapt_fixture(
                stem,
                tool=tool,
                tool_base=self.base_of_kind(tool, kind),
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
# Provenance. The two assertions that establish where a fixture's bytes came from, and
# the only ones in this module that read the runners' own artifacts.
#
# AAP 0.6.2 defines a positive fixture as "an unmodified captured excerpt" of the tool's
# own output, and gives the reason: a hand-written fixture tests the adapter against the
# shape someone believed the tool emits rather than the shape it emits. Every other class
# here asserts the adapter's behaviour on a fixture; this one asserts the fixture itself,
# against harness/artifacts/raw/<tool>.sarif, because a fixture's own sha256 -- recorded
# in the expected file that same fixture owns -- can only ever show that the file has not
# changed since that digest was taken, never where the bytes originated.
#
# "Byte for byte", for a JSON record, is asserted here as equality of
# json.dumps(obj, sort_keys=True): the raw artifacts are written as one compact line and
# the fixtures are indented for review, so file bytes cannot be compared directly, while
# a canonical serialization compares the record's whole structure -- every key, every
# value, every nesting level, and no key the fixture added or dropped. Two records that
# differ anywhere differ in it.
#
# Reading three artifacts totalling about 120 MB is done once per class, and only the
# comparison surface is kept: the canonical form of every record and rule, the envelope
# with those two arrays removed, and the counts. The parsed documents are released
# immediately, so the class holds tens of megabytes rather than the parse tree.
# --------------------------------------------------------------------------------------


def _canonical(value: Any) -> str:
    """Return the canonical serialization this module compares JSON records by.

    ``sort_keys`` makes the comparison independent of member order, which a JSON object
    does not define, while preserving every key and value. Array order is preserved,
    because a SARIF array's order is significant -- ``locations[0]`` is the first location
    and ``rules[i]`` is what a ``ruleIndex`` names.
    """
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _raw_artifact_path(tool: str) -> Path:
    """Return the runner's verbatim artifact for ``tool``."""
    return RAW_DIR / f"{tool}{FIXTURE_SUFFIX}"


class _RawView:
    """The comparison surface of one raw artifact, without its parse tree.

    Built once, from the artifact as the runner wrote it. ``results`` and
    ``tool.driver.rules`` are kept as canonical strings; every other envelope member is
    kept as a canonical string too, so an envelope difference is detectable without
    holding the document. The document itself is dropped once this is built.
    """

    __slots__ = (
        "tool", "path", "top_keys", "run_keys", "driver_keys", "result_canons",
        "rule_ids_in_order", "rule_canon_by_id", "rule_index_by_id", "top_members",
        "run_members", "driver_members", "has_original_uri_base_ids",
        "notification_count", "artifact_count",
    )

    def __init__(self, tool: str, document: Any) -> None:
        run = document["runs"][0]
        driver = run["tool"]["driver"]
        rules = driver.get("rules") or []
        self.tool = tool
        self.path = _raw_artifact_path(tool)
        self.top_keys = tuple(document)
        self.run_keys = tuple(run)
        self.driver_keys = tuple(driver)
        # Every top-level and run member except the two record-bearing arrays and the
        # tool object that holds the rules; those are compared element by element.
        self.top_members = {
            key: _canonical(value) for key, value in document.items() if key != "runs"
        }
        self.run_members = {
            key: _canonical(value)
            for key, value in run.items()
            if key not in ("results", "tool")
        }
        self.driver_members = {
            key: _canonical(value) for key, value in driver.items() if key != "rules"
        }
        self.result_canons = [_canonical(result) for result in run["results"]]
        self.rule_ids_in_order = tuple(rule.get("id") for rule in rules)
        self.rule_canon_by_id = {}
        self.rule_index_by_id = {}
        for index, rule in enumerate(rules):
            identifier = rule.get("id")
            if identifier not in self.rule_canon_by_id:
                self.rule_canon_by_id[identifier] = _canonical(rule)
                self.rule_index_by_id[identifier] = index
        self.has_original_uri_base_ids = "originalUriBaseIds" in run
        invocations = run.get("invocations")
        if isinstance(invocations, list) and invocations:
            notifications = invocations[0].get("toolExecutionNotifications")
            self.notification_count = (
                len(notifications) if isinstance(notifications, list) else None
            )
        else:
            self.notification_count = None
        artifacts = run.get("artifacts")
        self.artifact_count = len(artifacts) if isinstance(artifacts, list) else None


class RawArtifactProvenanceTests(SarifAdapterTestCase):
    """Each captured fixture is its tool's raw artifact; each derived fixture is not.

    Two assertions, and they are the ones that make every "unmodified captured excerpt"
    claim in this tree falsifiable:

    * every result and every retained rule in a captured fixture is the raw artifact's own
      object, identical under :func:`_canonical` and in raw document order, and the
      envelope around them -- keys and values, the notification array's full length, the
      artifacts array, and the *absence* of ``run.originalUriBaseIds`` -- is raw's own,
      with no member the fixture introduced;
    * every derived fixture's expectation declares it derived, and the fixture is not a
      faithful excerpt of the raw artifact, so a derived file can never be read as a
      capture.

    The second is not the trivial converse of the first. A derived fixture that happened
    to be a faithful excerpt would be miscategorised, and its expected file would be
    stating a provenance that understated what the file actually is.
    """

    #: Tool -> its raw artifact's comparison surface, built once for the class.
    raw_views: dict[str, _RawView]
    #: Tool -> why its raw artifact could not be read, where it could not be.
    raw_unavailable: dict[str, str]

    @classmethod
    def setUpClass(cls) -> None:
        """Build each tool's raw comparison surface once.

        A failure to read one is recorded rather than raised here, so that
        :meth:`test_every_raw_artifact_this_module_needs_is_present` reports it as a named
        failure against the path it looked for. Raising in class setup would report an
        error on every method in the class and bury the one fact that matters.
        """
        super().setUpClass()
        cls.raw_views = {}
        cls.raw_unavailable = {}
        for tool in sorted(set(ROW_FIXTURES.values())):
            path = _raw_artifact_path(tool)
            try:
                document = _read_json(path)
                cls.raw_views[tool] = _RawView(tool, document)
                del document
            except (OSError, ValueError, KeyError, IndexError, TypeError) as error:
                cls.raw_unavailable[tool] = f"{type(error).__name__}: {error}"

    def raw_view(self, tool: str) -> _RawView:
        """Return ``tool``'s raw comparison surface, failing explicitly if it is absent.

        No test in this class is skipped when an artifact is missing. A skip would leave
        the run record reporting a pass over a fixture whose provenance nobody checked,
        and the run record reports ``skipped=0`` as a property of this suite.
        """
        if tool not in self.raw_views:
            self.fail(
                f"the raw artifact for {tool!r} could not be read from "
                f"{_raw_artifact_path(tool)}: "
                f"{self.raw_unavailable.get(tool, 'no reason recorded')}. "
                "It is a git-tracked deliverable of this run and the provenance of "
                "fixtures/"
                f"{tool}{FIXTURE_SUFFIX} cannot be established without it. This is a "
                "condition to report, not one to repair from a test: nothing under "
                "harness/artifacts/ is created, cleared or edited here."
            )
        return self.raw_views[tool]

    @staticmethod
    def _difference(left: str, right: str) -> str:
        """Return a bounded description of where two canonical strings first differ.

        Bounded deliberately. A raw SARIF rule can serialize to several kilobytes and a
        full diff of two of them buries the one position that matters; the offset and a
        short window on each side name it precisely.
        """
        limit = min(len(left), len(right))
        offset = next((i for i in range(limit) if left[i] != right[i]), limit)
        window = slice(max(0, offset - 40), offset + 40)
        return (
            f"first difference at character {offset} of "
            f"{len(left)}/{len(right)}; fixture ...{left[window]}... raw ...{right[window]}..."
        )

    def provenance_defects(self, stem: str, tool: str) -> list[str]:
        """Return every way the fixture named ``stem`` departs from a faithful excerpt.

        One implementation, used in both directions: a captured fixture must produce an
        empty list, and a derived fixture must produce a non-empty one. Writing the check
        once is what keeps the two categories from being judged by different standards.

        A defect is any of: a top-level or ``runs[0]`` or ``tool.driver`` key the raw
        artifact does not have or does have; a non-record member whose value differs; a
        result that is not a raw result object, or one that appears out of raw document
        order; a retained rule that is not the raw rule with that identifier; a
        ``run.originalUriBaseIds`` whose presence differs from raw's; and a truncated
        ``toolExecutionNotifications`` or ``artifacts`` array.
        """
        view = self.raw_view(tool)
        document = _read_json(_fixture_path(stem))
        defects: list[str] = []

        if tuple(document) != view.top_keys:
            defects.append(
                f"top-level keys {tuple(document)!r} differ from raw's {view.top_keys!r}"
            )
        for key, value in document.items():
            if key == "runs":
                continue
            if key not in view.top_members:
                defects.append(f"top-level member {key!r} is not in the raw artifact")
            elif _canonical(value) != view.top_members[key]:
                defects.append(f"top-level member {key!r} differs from raw's")
        for key in view.top_members:
            if key not in document:
                defects.append(f"top-level member {key!r} of the raw artifact is absent")

        runs = document.get("runs")
        if not isinstance(runs, list) or len(runs) != 1:
            defects.append("the fixture does not carry exactly one run, as raw does")
            return defects
        run = runs[0]
        if tuple(run) != view.run_keys:
            defects.append(
                f"runs[0] keys {tuple(run)!r} differ from raw's {view.run_keys!r}"
            )
        for key, value in run.items():
            if key in ("results", "tool"):
                continue
            if key not in view.run_members:
                defects.append(f"runs[0] member {key!r} is not in the raw artifact")
            elif _canonical(value) != view.run_members[key]:
                defects.append(f"runs[0] member {key!r} differs from raw's")
        for key in view.run_members:
            if key not in run:
                defects.append(f"runs[0] member {key!r} of the raw artifact is absent")
        if ("originalUriBaseIds" in run) != view.has_original_uri_base_ids:
            defects.append(
                "run.originalUriBaseIds is "
                f"{'present' if 'originalUriBaseIds' in run else 'absent'} here and "
                f"{'present' if view.has_original_uri_base_ids else 'absent'} in raw"
            )

        driver = run.get("tool", {}).get("driver", {})
        if tuple(driver) != view.driver_keys:
            defects.append(
                f"tool.driver keys {tuple(driver)!r} differ from raw's "
                f"{view.driver_keys!r}"
            )
        for key, value in driver.items():
            if key == "rules":
                continue
            if key not in view.driver_members:
                defects.append(f"tool.driver member {key!r} is not in the raw artifact")
            elif _canonical(value) != view.driver_members[key]:
                defects.append(f"tool.driver member {key!r} differs from raw's")

        previous = -1
        for index, result in enumerate(run.get("results") or []):
            canonical = _canonical(result)
            position = next(
                (
                    i
                    for i, raw_canonical in enumerate(view.result_canons)
                    if raw_canonical == canonical and i > previous
                ),
                None,
            )
            if position is None:
                if canonical in view.result_canons:
                    defects.append(
                        f"result {index} appears in the raw artifact but out of raw "
                        f"document order, after raw index {previous}"
                    )
                else:
                    defects.append(
                        f"result {index} is not a raw result object: no raw result is "
                        "identical to it"
                    )
            else:
                previous = position

        for index, rule in enumerate(driver.get("rules") or []):
            identifier = rule.get("id")
            if identifier not in view.rule_canon_by_id:
                defects.append(
                    f"rule {index} carries id {identifier!r}, which the raw artifact's "
                    "rules array does not"
                )
            elif _canonical(rule) != view.rule_canon_by_id[identifier]:
                defects.append(
                    f"rule {index} ({identifier!r}) differs from the raw rule with that "
                    "id"
                )
        return defects

    # -- the captured fixtures ---------------------------------------------------------

    def test_every_raw_artifact_this_module_needs_is_present(self) -> None:
        """Each tool's raw artifact exists, parses, and is a SARIF envelope with a run.

        Failure names the path it looked for. This runs as a test rather than as a skip
        condition precisely so that a missing artifact is reported as a failure of this
        suite rather than as coverage nobody notices is gone.
        """
        for tool in sorted(set(ROW_FIXTURES.values())):
            with self.subTest(tool=tool):
                path = _raw_artifact_path(tool)
                self.assertTrue(
                    path.is_file(),
                    msg=(
                        f"the raw artifact {path} is absent. It is the provenance "
                        f"authority for fixtures/{tool}{FIXTURE_SUFFIX} and is a "
                        "git-tracked deliverable of this run"
                    ),
                )
                view = self.raw_view(tool)
                self.assertIn(
                    "version",
                    view.top_members,
                    msg=f"{path}: no top-level version member",
                )
                self.assertEqual(
                    view.top_members["version"],
                    _canonical("2.1.0"),
                    msg=f"{path}: not a SARIF 2.1.0 envelope",
                )
                self.assertTrue(
                    view.result_canons,
                    msg=f"{path}: the artifact carries no results to excerpt",
                )

    def test_every_captured_result_is_the_raw_object_in_raw_document_order(self) -> None:
        """Assertion 23: each selected record is raw's own record, and the order is raw's.

        Selection is the only freedom AAP 0.6.2 allows -- whole records, chosen -- so both
        halves matter. Identity under :func:`_canonical` is the operative meaning of "byte
        for byte" for a JSON record, since the artifact is written compact and the fixture
        indented. Order is asserted by requiring each match to sit at a strictly greater
        raw index than the last: a fixture that reordered its records would still contain
        raw objects, and reading it would misrepresent the artifact's own sequence.
        """
        for stem, tool in POSITIVE_FIXTURES.items():
            view = self.raw_view(tool)
            document = _read_json(_fixture_path(stem))
            results = document["runs"][0]["results"]
            with self.subTest(fixture=stem):
                self.assertTrue(results, msg=f"{stem}: the fixture selects no records")
                previous = -1
                positions: list[int] = []
                for index, result in enumerate(results):
                    canonical = _canonical(result)
                    position = next(
                        (
                            i
                            for i, raw_canonical in enumerate(view.result_canons)
                            if raw_canonical == canonical and i > previous
                        ),
                        None,
                    )
                    self.assertIsNotNone(
                        position,
                        msg=(
                            f"{stem}: result {index} is not an unmodified record of "
                            f"{view.path.name} at an index above {previous}. Either it "
                            "was edited, or it was reordered; both are the edit AAP 0.6.2 "
                            "forbids in a positive fixture"
                        ),
                    )
                    positions.append(position)
                    previous = position
                self.assertEqual(
                    positions,
                    sorted(positions),
                    msg=f"{stem}: the selected records are not in raw document order",
                )
                self.assertEqual(
                    len(set(positions)),
                    len(positions),
                    msg=(
                        f"{stem}: two selected records matched one raw record, so the "
                        "selection is not a set of whole distinct records"
                    ),
                )

    def test_every_retained_rule_is_the_raw_rule_with_the_same_id(self) -> None:
        """Assertion 23, continued: rule metadata is raw's, not a rewritten subset.

        A rule object is where the severity, the CWE tokens and the help text come from, so
        a rule "trimmed to what the test needs" silently changes the answer the adapter
        gives. Each retained rule is compared with the raw rule carrying the same
        identifier, and the retained sequence is required to be in raw's own order -- which
        is what keeps a ``ruleIndex`` meaningful.

        Where a fixture retains the artifact's complete rules array, that is asserted as
        completeness rather than assumed: the ``datadog-static-analyzer`` results state
        absolute indexes into it, and a subset would have required renumbering the very
        field a capture must not touch.
        """
        for stem, tool in POSITIVE_FIXTURES.items():
            view = self.raw_view(tool)
            document = _read_json(_fixture_path(stem))
            run = document["runs"][0]
            rules = run["tool"]["driver"].get("rules") or []
            results = run["results"]
            with self.subTest(fixture=stem):
                self.assertTrue(rules, msg=f"{stem}: no rules retained at all")
                indexes: list[int] = []
                for index, rule in enumerate(rules):
                    identifier = rule.get("id")
                    self.assertIn(
                        identifier,
                        view.rule_canon_by_id,
                        msg=(
                            f"{stem}: rule {index} carries id {identifier!r}, which "
                            f"{view.path.name} does not"
                        ),
                    )
                    same = _canonical(rule) == view.rule_canon_by_id[identifier]
                    self.assertTrue(
                        same,
                        msg=(
                            f"{stem}: rule {index} ({identifier!r}) differs from the raw "
                            "rule with that id -- "
                            + self._difference(
                                _canonical(rule), view.rule_canon_by_id[identifier]
                            )
                        ),
                    )
                    indexes.append(view.rule_index_by_id[identifier])
                self.assertEqual(
                    indexes,
                    sorted(indexes),
                    msg=f"{stem}: the retained rules are not in raw document order",
                )
                if len(rules) == len(view.rule_ids_in_order):
                    self.assertEqual(
                        tuple(rule.get("id") for rule in rules),
                        view.rule_ids_in_order,
                        msg=(
                            f"{stem}: the fixture retains as many rules as the artifact "
                            "has but not the same ones in the same order"
                        ),
                    )
                # Every ruleIndex a retained result states must still name its own rule.
                for index, result in enumerate(results):
                    stated = result.get("ruleIndex")
                    if not isinstance(stated, int) or isinstance(stated, bool):
                        continue
                    self.assertLess(
                        stated,
                        len(rules),
                        msg=(
                            f"{stem}: result {index} states ruleIndex {stated}, which is "
                            "outside the retained rules array -- a capture must keep the "
                            "index resolvable rather than renumber it"
                        ),
                    )
                    self.assertEqual(
                        rules[stated].get("id"),
                        result.get("ruleId"),
                        msg=(
                            f"{stem}: result {index} states ruleIndex {stated}, which "
                            "names a different rule than its own ruleId. The index was "
                            "renumbered, which is the alteration this test exists to catch"
                        ),
                    )

    def test_the_captured_envelope_is_raws_own_and_introduces_no_member(self) -> None:
        """Assertion 23, continued: the structure around the records is the artifact's.

        Everything a reader would take on trust: the top-level key set and every non-record
        value in it, ``$schema`` present exactly where raw has it and with raw's value, the
        ``runs[0]`` key set, the driver's own metadata, the complete
        ``toolExecutionNotifications`` array where the producer emits one, the complete
        ``artifacts`` array where it emits one, and the absence of
        ``run.originalUriBaseIds`` where the producer emits none -- which is what makes the
        documented degenerate-base fallback the live path rather than an authored base map.
        """
        for stem, tool in POSITIVE_FIXTURES.items():
            view = self.raw_view(tool)
            document = _read_json(_fixture_path(stem))
            run = document["runs"][0]
            driver = run["tool"]["driver"]
            with self.subTest(fixture=stem):
                self.assertEqual(
                    tuple(document),
                    view.top_keys,
                    msg=f"{stem}: the top-level key set is not {view.path.name}'s",
                )
                self.assertEqual(
                    tuple(run),
                    view.run_keys,
                    msg=f"{stem}: the runs[0] key set is not {view.path.name}'s",
                )
                self.assertEqual(
                    tuple(driver),
                    view.driver_keys,
                    msg=f"{stem}: the tool.driver key set is not {view.path.name}'s",
                )
                self.assertEqual(
                    ("$schema" in document),
                    ("$schema" in view.top_members),
                    msg=(
                        f"{stem}: $schema is "
                        f"{'present' if '$schema' in document else 'absent'} here and "
                        f"{'present' if '$schema' in view.top_members else 'absent'} in "
                        f"{view.path.name}. An authored $schema is a member the producer "
                        "did not write"
                    ),
                )
                for key, value in document.items():
                    if key == "runs":
                        continue
                    self.assertEqual(
                        _canonical(value),
                        view.top_members[key],
                        msg=f"{stem}: top-level member {key!r} differs from raw's",
                    )
                for key, value in run.items():
                    if key in ("results", "tool"):
                        continue
                    self.assertEqual(
                        _canonical(value),
                        view.run_members[key],
                        msg=(
                            f"{stem}: runs[0] member {key!r} differs from raw's -- "
                            + self._difference(
                                _canonical(value), view.run_members[key]
                            )
                        ),
                    )
                for key, value in driver.items():
                    if key == "rules":
                        continue
                    self.assertEqual(
                        _canonical(value),
                        view.driver_members[key],
                        msg=f"{stem}: tool.driver member {key!r} differs from raw's",
                    )
                self.assertNotIn(
                    "originalUriBaseIds",
                    run,
                    msg=(
                        f"{stem}: the fixture carries a base map that {view.path.name} "
                        "does not. That is the authored member F3 named, and it changes "
                        "which resolution branch every row takes"
                    ),
                )
                self.assertFalse(
                    view.has_original_uri_base_ids,
                    msg=(
                        f"{view.path.name} now emits originalUriBaseIds, so this "
                        "assertion's premise has changed and the fixture must be "
                        "recaptured with it -- a producer change to record, not to "
                        "suppress"
                    ),
                )
                if view.notification_count is not None:
                    notifications = run["invocations"][0]["toolExecutionNotifications"]
                    self.assertEqual(
                        len(notifications),
                        view.notification_count,
                        msg=(
                            f"{stem}: the fixture carries {len(notifications)} of "
                            f"{view.path.name}'s {view.notification_count} "
                            "toolExecutionNotifications. A truncated notification array is "
                            "an edited envelope: it is the runner's own record of what the "
                            "tool reported about its execution"
                        ),
                    )
                if view.artifact_count is not None:
                    self.assertEqual(
                        len(run["artifacts"]),
                        view.artifact_count,
                        msg=(
                            f"{stem}: the fixture carries {len(run['artifacts'])} of "
                            f"{view.path.name}'s {view.artifact_count} artifacts entries"
                        ),
                    )

    def test_every_captured_fixture_is_a_faithful_excerpt(self) -> None:
        """The aggregate: no defect at all, by the same test the derived fixtures fail.

        The three preceding methods name each aspect so a failure says which one broke.
        This one runs the shared :meth:`provenance_defects` check and requires it empty, so
        that the standard a capture is held to and the standard a derived fixture is
        measured against are literally the same code.
        """
        for stem, tool in POSITIVE_FIXTURES.items():
            with self.subTest(fixture=stem):
                defects = self.provenance_defects(stem, tool)
                self.assertEqual(
                    defects,
                    [],
                    msg=(
                        f"{stem} is committed as an unmodified captured excerpt of "
                        f"{_raw_artifact_path(tool).name} but departs from it: "
                        + "; ".join(defects)
                    ),
                )

    def test_every_captured_expectation_claims_the_capture_and_the_raw_indexes_it_names(
        self,
    ) -> None:
        """Each captured expectation's provenance claims are true of the file it describes.

        Two claims are checked rather than read. The ``fixture`` block must name the raw
        artifact as its source and must not carry a derived declaration -- the false
        ``excerpt_note`` on the opengrep expectation was exactly this: a claim of
        unmodified capture over a fixture whose notification array had been truncated. And
        every ``raw_artifact_result_index`` and ``raw_artifact_rule_index`` the row
        derivations record must resolve, in the raw artifact, to the object the fixture
        carries at the pointer beside it. A recorded index nobody resolves is a citation to
        a line number that may not exist.
        """
        for stem, tool in POSITIVE_FIXTURES.items():
            view = self.raw_view(tool)
            expectation = _read_json(_expected_path(stem))
            document = _read_json(_fixture_path(stem))
            block = expectation["fixture"]
            with self.subTest(fixture=stem):
                self.assertEqual(
                    block.get("excerpt_of"),
                    f"harness/artifacts/raw/{tool}{FIXTURE_SUFFIX}",
                    msg=f"{stem}: the expectation does not name the raw artifact it excerpts",
                )
                self.assertEqual(
                    block.get("excerpt_kind"),
                    "unmodified captured excerpt",
                    msg=f"{stem}: the expectation does not claim an unmodified excerpt",
                )
                self.assertNotIn(
                    "provenance",
                    block,
                    msg=(
                        f"{stem}: a captured expectation carries no derived-provenance "
                        "block; that block belongs to the derived fixtures"
                    ),
                )
                self.assertEqual(
                    block.get("results"),
                    len(document["runs"][0]["results"]),
                    msg=f"{stem}: the recorded result count is not the fixture's",
                )
                self.assertEqual(
                    block.get("driver_rules"),
                    len(document["runs"][0]["tool"]["driver"]["rules"]),
                    msg=f"{stem}: the recorded rule count is not the fixture's",
                )
                for derivation in expectation["row_derivations"]:
                    raw_result_index = derivation["raw_artifact_result_index"]
                    self.assertEqual(
                        _canonical(
                            _json_pointer(document, derivation["result_pointer"])
                        ),
                        view.result_canons[raw_result_index],
                        msg=(
                            f"{stem}: row {derivation['row_index']} records raw result "
                            f"index {raw_result_index}, which is not the record at "
                            f"{derivation['result_pointer']}"
                        ),
                    )
                    rule = _json_pointer(document, derivation["rule_pointer"])
                    self.assertEqual(
                        view.rule_index_by_id[rule["id"]],
                        derivation["raw_artifact_rule_index"],
                        msg=(
                            f"{stem}: row {derivation['row_index']} records raw rule index "
                            f"{derivation['raw_artifact_rule_index']} for rule "
                            f"{rule['id']!r}, which sits at "
                            f"{view.rule_index_by_id[rule['id']]} in the raw artifact"
                        ),
                    )

    # -- the derived fixtures ----------------------------------------------------------

    def test_every_derived_fixture_is_declared_derived_and_is_not_a_raw_excerpt(
        self,
    ) -> None:
        """Assertion 24: a derived fixture can never be mistaken for a capture.

        Both halves are needed. The declaration is what a reader sees -- the expectation
        states ``kind`` as a derived fixture, sets ``declared_derived``, names what it was
        derived from and lists the authored feature cases it exists to exercise, and makes
        no ``excerpt_kind`` claim. The measurement is what makes the declaration
        falsifiable: the same :meth:`provenance_defects` check the captures must pass
        cleanly must report at least one defect here, and the defects are what the
        expectation's ``alterations`` list describes in prose.

        A derived fixture that turned out to be a faithful excerpt would fail this, and
        rightly: it would be a capture filed under the wrong provenance, and its expected
        file would be understating what the file is.
        """
        for stem, tool in DERIVED_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            block = expectation["fixture"]
            provenance = block.get("provenance")
            with self.subTest(fixture=stem):
                self.assertIsInstance(
                    provenance,
                    dict,
                    msg=f"{stem}: the expectation carries no fixture.provenance block",
                )
                self.assertTrue(
                    provenance.get("declared_derived") is True,
                    msg=(
                        f"{stem}: the expectation does not set declared_derived true, so "
                        "nothing marks the file as authored material"
                    ),
                )
                self.assertIn(
                    "derived",
                    str(provenance.get("kind", "")).lower(),
                    msg=f"{stem}: provenance.kind does not say the fixture is derived",
                )
                self.assertNotIn(
                    "excerpt_kind",
                    block,
                    msg=(
                        f"{stem}: a derived fixture must make no captured-excerpt claim; "
                        "AAP 0.6.2 reserves that word for unmodified captured output"
                    ),
                )
                for key in ("derived_from", "ultimate_source", "alterations"):
                    self.assertIn(
                        key,
                        provenance,
                        msg=f"{stem}: provenance records no {key!r}",
                    )
                self.assertTrue(
                    provenance["alterations"],
                    msg=f"{stem}: provenance lists no alteration, so nothing is declared",
                )
                defects = self.provenance_defects(stem, tool)
                self.assertNotEqual(
                    defects,
                    [],
                    msg=(
                        f"{stem} is declared derived but is a faithful excerpt of "
                        f"{_raw_artifact_path(tool).name}. A capture filed as derived "
                        "understates what the file is, and the expectation's provenance "
                        "block is then wrong in the opposite direction"
                    ),
                )

    def test_every_derived_fixture_carries_a_case_no_capture_supplies(self) -> None:
        """A derived fixture earns its place by covering something a capture cannot.

        Otherwise it is duplicated coverage that leaves nothing better tested, and the case
        for splitting the file falls away. The check is behavioural rather than textual:
        the derived fixture must move at least one counter that no captured fixture moves,
        with the counters read from the adapter on each fixture rather than from the
        expected files.
        """
        captured_totals: dict[str, int] = {}
        for stem, tool in POSITIVE_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            _rows, _rejections, counters, _tally = self.adapt_fixture(
                stem,
                tool=tool,
                tool_base=self.base_of_kind(tool, expected_path_base_kind(expectation)),
            )
            for key, value in counters.items():
                captured_totals[key] = captured_totals.get(key, 0) + value
        for stem, tool in DERIVED_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            with self.subTest(fixture=stem):
                _rows, _rejections, counters, _tally = self.adapt_fixture(
                    stem,
                    tool=tool,
                    tool_base=self.base_of_kind(
                        tool, expected_path_base_kind(expectation)
                    ),
                )
                exclusive = sorted(
                    key
                    for key, value in counters.items()
                    if value > 0 and captured_totals.get(key, 0) == 0
                )
                self.assertTrue(
                    exclusive,
                    msg=(
                        f"{stem} moves no counter the captured fixtures leave at zero, so "
                        "it adds no coverage the captures do not already have"
                    ),
                )


# --------------------------------------------------------------------------------------
# Assertions 1, 2, 3 -- and 9 to 11 wherever rows appear.
# --------------------------------------------------------------------------------------


class PositiveRowTests(SarifAdapterTestCase):
    """Every row of every row-producing fixture, field by field, against its expectation.

    The inventory is :data:`ROW_FIXTURES`: the three captured positives and the two
    derived features fixtures. Both kinds carry a hand-verified expectation and both must
    map field for field; what separates them is provenance, which
    :class:`RawArtifactProvenanceTests` owns, not whether their rows are asserted.
    """

    def test_rows_match_the_expected_file_field_by_field(self) -> None:
        """Assertions 1 and 2: the row count, then all twelve fields in order.

        The context is the shipped one: this tool's recorded base and the recorded scan
        root. Each expected file states that its rows were derived under exactly that,
        and :meth:`base_of_kind` fails loudly if the metadata has since changed.
        """
        for stem, tool in ROW_FIXTURES.items():
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
                        f"{stem}: a positive-mapping fixture produces no rejection; got "
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
        for stem, tool in ROW_FIXTURES.items():
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
        for stem, tool in ROW_FIXTURES.items():
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
        """``records walked == rows + rejections`` on every row-producing fixture.

        The left side is counted by a traversal that builds nothing, so the identity is
        checked from a second implementation rather than from the adapter's own bookkeeping.
        """
        for stem, tool in ROW_FIXTURES.items():
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
        """Assertions 9, 10 and 11 over every row of every row-producing fixture."""
        for stem, tool in ROW_FIXTURES.items():
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
        tool = ROW_FIXTURES[stem]
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
        for stem in ROW_FIXTURES:
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
        for stem in ROW_FIXTURES:
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
        for stem in ROW_FIXTURES:
            tool = ROW_FIXTURES[stem]
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
        for stem in ROW_FIXTURES:
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
        for stem in ROW_FIXTURES:
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
        for stem in ROW_FIXTURES:
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

    def test_both_routes_are_exercised_by_the_committed_fixtures(self) -> None:
        """A committed fixture covers each route, and the counters say which was taken.

        SARIF 2.1.0 lets a result identify its rule either directly or by index into
        ``runs[].tool.driver.rules[]``. Both are live somewhere in the committed
        row-producing fixtures, and the adapter counts them separately, so the counters are
        the evidence that each route was actually taken rather than one route serving
        twice.

        The inventory iterated is :data:`ROW_FIXTURES` -- the captures and the derived
        features fixtures -- rather than the captures alone, because the indexed route is
        measurably unreachable from captured output in this provisioning: not one result in
        the raw ``opengrep`` or ``semgrep`` artifact carries a ``ruleIndex`` at all, and
        every result in the raw ``datadog-static-analyzer`` artifact carries a ``ruleId``
        beside its index, which outranks it. The route is therefore supplied by
        ``derived-semgrep-features``, whose expected file records that as the reason it
        exists. The inventory the sum is taken over is part of the assertion: a sum over
        the captures alone would claim coverage the captured artifacts cannot supply.

        The two counters are summed across the fixtures rather than read per fixture, and
        deliberately so: the question is whether the adapter's two routes are exercised at
        all, not which artifact exercises which. Reading them per tool would invite a
        comparison between producers, and this module makes none. The fixture supplying
        each route is nonetheless recorded below, because a sum that reached one cannot say
        which member moved.
        """
        totals = {sarif.COUNTER_RULE_ID_FROM_RULE_ID: 0,
                  sarif.COUNTER_RULE_ID_FROM_RULE_INDEX: 0}
        supplied_by: dict[str, list[str]] = {key: [] for key in totals}
        for stem, tool in ROW_FIXTURES.items():
            expectation = _read_json(_expected_path(stem))
            _rows, _rejections, counters, _tally = self.adapt_fixture(
                stem,
                tool=tool,
                tool_base=self.base_of_kind(tool, expected_path_base_kind(expectation)),
            )
            for key in totals:
                totals[key] += counters[key]
                if counters[key] > 0:
                    supplied_by[key].append(stem)
        for key, total in totals.items():
            with self.subTest(counter=key):
                self.assertGreater(
                    total,
                    0,
                    msg=(
                        f"no committed fixture exercises {key!r}; one of the two rule "
                        "identifier routes is untested"
                    ),
                )
                self.assertTrue(
                    supplied_by[key],
                    msg=f"{key!r} reached {total} with no fixture recorded as its source",
                )
        self.assertEqual(
            supplied_by[sarif.COUNTER_RULE_ID_FROM_RULE_INDEX],
            ["derived-semgrep-features"],
            msg=(
                "the indexed route is expected to come from the derived fixture that "
                "declares it, and from that fixture alone; it came from "
                f"{supplied_by[sarif.COUNTER_RULE_ID_FROM_RULE_INDEX]!r}. If a capture now "
                "carries a result with a ruleIndex and no ruleId, that is a change in the "
                "producer's output to record rather than a failure to suppress"
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

    # -- the two descriptors, compared (F13) -------------------------------------------
    #
    # Where a result carries ``ruleId`` *and* ``ruleIndex``, SARIF 2.1.0 sections 3.27.5
    # and 3.27.6 make them two references to one ``reportingDescriptor``, so the indexed
    # rule's ``id`` is the identifier ``ruleId`` states. A consumer that resolves both and
    # compares neither takes the row's identity from one rule and its severity, CWE and CVE
    # from another, and the resulting row describes a finding no rule in the artifact
    # reports (CWE-345). Nothing about such a row looks wrong, which is why the assertions
    # below are on the rejection and on the counter rather than on a corrupted field.
    #
    # The four methods separate the one shape that is a contradiction from the three that
    # are not. Getting that boundary wrong in the lenient direction restores the defect;
    # getting it wrong in the strict direction rejects records whose descriptors were never
    # shown to conflict, and the 6,832 datadog-static-analyzer results in
    # harness/artifacts/raw/ are the measurement that says how much traffic the boundary
    # carries: every one of them emits both descriptors, and every one agrees.

    def test_a_disagreeing_rule_id_and_rule_index_are_a_counted_rejection(self) -> None:
        """Both descriptors present and naming different rules: rejected, not resolved.

        Neither descriptor is preferred, because nothing in the document says which one the
        producer meant: taking ``ruleId`` would keep an identifier whose metadata cannot be
        trusted, and taking the indexed rule would rename the finding. Both are the
        inference AAP 0.5.4 forbids, so the record is rejected under ``malformed_record``
        -- it contradicts the format it declares -- and counted.

        The detail is asserted for the three things a reader needs in order to find the
        contradiction in the artifact without re-deriving it: both identifiers and the index.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", rule_index=0)],
            rules=[{"id": "authored.rule.zero"}, {"id": "authored.rule.one"}],
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(
            rows,
            [],
            msg=(
                "a record whose two rule descriptors disagree must produce no row; a row "
                "here would carry one rule's identifier and the other's metadata"
            ),
        )
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_MALFORMED_RECORD,
            label="authored disagreeing ruleId and ruleIndex",
        )
        detail = rejections[0].detail
        for substring in (
            "'authored.rule.one'",
            "'authored.rule.zero'",
            "ruleIndex 0",
            "tool.driver.rules[0]",
        ):
            with self.subTest(substring=substring):
                self.assertIn(
                    substring,
                    detail,
                    msg=(
                        "the detail must name both identifiers and the index, so the "
                        f"contradiction is locatable; {substring!r} is absent from {detail!r}"
                    ),
                )
        self.assertEqual(
            counters[sarif.COUNTER_RULE_INDEX_UNUSABLE],
            1,
            msg=(
                "the contradiction must be counted under the adapter's existing "
                "rule-index counter, so the run record carries the count the way it "
                "carries the other rule-resolution counts"
            ),
        )
        self.assertEqual(
            counters[sarif.COUNTER_RULE_ID_FROM_RULE_ID],
            0,
            msg=(
                "no identifier was resolved -- the two candidates disagreed -- so neither "
                "identifier counter may move"
            ),
        )
        self.assertEqual(counters[sarif.COUNTER_RULE_ID_FROM_RULE_INDEX], 0)

    def test_indexed_metadata_is_read_only_once_the_ids_are_equal(self) -> None:
        """The metadata the indexed rule carries is reached only after equality is proven.

        Two rules whose ``properties`` differ visibly -- a distinct ``cwe`` and a distinct
        ``severity`` on each -- so that accepting a disagreeing pair would leave a mark
        rather than merely being wrong in principle. On the agreeing pair the row takes the
        indexed rule's ``cwe``, which is what proves the metadata is still read on the
        ordinary path and that this check has not simply stopped the indexed route working.

        The severity is deliberately left to ``level`` on both records rather than
        contrasted through the rules: ``level`` outranks a rule property, so a difference
        there would be invisible and would make the assertion vacuous.
        """
        rules = [
            {"id": "authored.rule.zero", "properties": {"cwe": "CWE-22"}},
            {"id": "authored.rule.one", "properties": {"cwe": "CWE-79"}},
        ]
        agreeing = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", rule_index=1)],
            rules=rules,
        )
        rows, rejections, counters, _tally = self.adapt(agreeing, tool="opengrep")
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["rule_id"], "authored.rule.one")
        self.assertEqual(
            rows[0]["cwe"],
            "CWE-79",
            msg=(
                "on an agreeing pair the indexed rule's metadata must still be read; "
                "refusing it here would have turned a consistency check into a regression"
            ),
        )
        self.assertEqual(counters[sarif.COUNTER_RULE_INDEX_UNUSABLE], 0)
        self.assert_schema_invariants(rows, label="authored agreeing pair")

        disagreeing = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", rule_index=0)],
            rules=rules,
        )
        rows, rejections, _counters, _tally = self.adapt(disagreeing, tool="opengrep")
        self.assertEqual(
            rows,
            [],
            msg=(
                "the same pair made inconsistent must yield no row, and in particular no "
                "row carrying rule_id 'authored.rule.one' with CWE-22 from rules[0]"
            ),
        )
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_MALFORMED_RECORD,
            label="authored disagreeing pair with contrasting metadata",
        )

    def test_an_indexed_rule_with_no_id_is_unusable_rather_than_a_contradiction(self) -> None:
        """Equality unprovable is not equality, and it is not a contradiction either.

        The indexed rule resolves but declares no identifier of its own, so it can be shown
        neither to be nor not to be the rule ``ruleId`` names. Treating that as equal is the
        silent attachment this check exists to stop; treating it as a contradiction would
        reject a record whose descriptors were never shown to conflict. So the index is
        recorded unusable, its metadata is left unread, and the identifier from ``ruleId``
        stands -- the record is kept.

        The unread metadata is what the ``cwe`` assertion pins: the id-less rule carries
        one, the declaring rule does not, and the row must carry none.
        """
        document = authored_document(
            [authored_result(DISK_STORE_PATH, rule_id="authored.rule.one", rule_index=0)],
            rules=[{"properties": {"cwe": "CWE-22"}}, {"id": "authored.rule.one"}],
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(
            rejections,
            [],
            msg="an index that cannot be compared is a reason, not a rejection",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["rule_id"], "authored.rule.one")
        self.assertIsNone(
            rows[0]["cwe"],
            msg=(
                "the id-less rule's metadata must not be read: its CWE-22 reaching the row "
                "would be exactly the wrong-rule attachment the check forbids"
            ),
        )
        self.assertEqual(counters[sarif.COUNTER_RULE_INDEX_UNUSABLE], 1)
        self.assertEqual(counters[sarif.COUNTER_RULE_ID_FROM_RULE_ID], 1)
        self.assert_schema_invariants(rows, label="authored id-less indexed rule")

    def test_an_unresolved_tool_component_falls_back_rather_than_rejecting(self) -> None:
        """An index whose component was never resolved yields no second identifier.

        The pre-existing defensive path, asserted so that the new comparison cannot absorb
        it. The ``rule.toolComponent`` reference names a ``guid`` no component in the run
        carries, so the index is not applied to the driver's rules -- an index is scoped to
        its component, and reading the wrong component's rule would be the same wrong-rule
        attachment by another route. Nothing was resolved to disagree with ``ruleId``, so
        this is a fall back and a pair of counted reasons, not a contradiction.
        """
        result = authored_result(
            DISK_STORE_PATH, rule_id="authored.rule.one", rule_index=0
        )
        result["rule"] = {
            "toolComponent": {"guid": "1f0dbc2c-0000-4000-8000-000000000000"}
        }
        document = authored_document(
            [result],
            rules=[{"id": "authored.rule.zero", "properties": {"cwe": "CWE-22"}},
                   {"id": "authored.rule.one"}],
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(
            rejections,
            [],
            msg=(
                "an unresolved component must stay the defensive fall back it already was; "
                "turning it into a rejection would refuse a record on an absence"
            ),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["rule_id"], "authored.rule.one")
        self.assertIsNone(
            rows[0]["cwe"],
            msg="rules[0] is a different rule and its CWE must not reach this row",
        )
        self.assertEqual(counters[sarif.COUNTER_TOOL_COMPONENT_UNRESOLVED], 1)
        self.assertEqual(counters[sarif.COUNTER_RULE_INDEX_UNUSABLE], 1)
        self.assertEqual(counters[sarif.COUNTER_RULE_ID_FROM_RULE_ID], 1)
        self.assert_schema_invariants(rows, label="authored unresolved toolComponent")

    def test_a_rule_reference_id_and_index_are_compared_too(self) -> None:
        """The same contradiction expressed through ``rule.id`` and ``rule.index``.

        A result may carry its descriptors on a ``rule`` reporting-descriptor reference
        instead of at the top level, and the comparison has to reach that spelling too --
        otherwise a producer switching to it would silently regain the defect. No result in
        any of this provisioning's three captured artifacts carries such a reference,
        measured over all 9,316 of their results, so the case is authored here rather than
        given a derived fixture: a fixture for it would be an authored shape under a derived
        name.

        The detail must name the members it actually read, ``rule.id`` and ``rule.index``,
        rather than the top-level spellings the record does not carry.
        """
        result = authored_result(DISK_STORE_PATH, rule_id=None, rule_index=None)
        result["rule"] = {"id": "authored.rule.one", "index": 0}
        document = authored_document(
            [result],
            rules=[{"id": "authored.rule.zero"}, {"id": "authored.rule.one"}],
        )
        rows, rejections, counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_MALFORMED_RECORD,
            label="authored disagreeing rule.id and rule.index",
        )
        detail = rejections[0].detail
        for substring in ("rule.id", "rule.index 0", "'authored.rule.one'",
                          "'authored.rule.zero'"):
            with self.subTest(substring=substring):
                self.assertIn(substring, detail)
        self.assertEqual(counters[sarif.COUNTER_RULE_INDEX_UNUSABLE], 1)

    def test_the_rule_index_mismatch_fixture_rejects_only_the_contradicting_record(self) -> None:
        """The committed fixture: one rejection, and its two neighbours still become rows.

        The generic negative loop already asserts this fixture against its whole expected
        file. This method states the two properties that make the fixture worth committing,
        so that neither can be lost to a change in the generic loop's inventory: the
        rejected record's identifier reaches no row, and the record *after* it is emitted --
        which is the partial-parse boundary, since a walk that abandoned the artifact at the
        contradiction could not have produced the second row.

        The fixture is ``datadog-static-analyzer`` output because that is the only one of
        the three producers whose results carry both descriptors, which
        :data:`NEGATIVE_FIXTURE_TOOLS` records as the reason.
        """
        stem = "reject-sarif-rule-index-mismatch"
        expectation = _read_json(_expected_path(stem))
        tool = negative_fixture_tool(stem)
        self.assertEqual(tool, "datadog-static-analyzer")
        rows, rejections, counters, _tally = self.adapt_fixture(
            stem,
            tool=tool,
            tool_base=self.base_of_kind(tool, expected_path_base_kind(expectation)),
        )
        self.assert_rows_match(rows, expectation["rows"], label=stem)
        self.assert_schema_invariants(rows, label=stem)
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_MALFORMED_RECORD,
            label=stem,
        )
        conflict = expectation["rejections"][0]["descriptors_in_conflict"]
        self.assertNotIn(
            conflict["rule_id"],
            [row["rule_id"] for row in rows],
            msg=(
                "the rejected record's ruleId reached a row, so the contradiction was "
                "resolved into the dataset rather than counted out of it"
            ),
        )
        self.assertNotIn(
            conflict["indexed_rule_id"],
            [row["rule_id"] for row in rows if row["start_line"] == 188],
            msg="no row may carry the indexed rule's identifier at the rejected location",
        )
        self.assertEqual(
            [row["start_line"] for row in rows],
            [74, 53],
            msg=(
                "the rows must straddle the rejection -- the record before it and the "
                "record after it -- so the partial parse is demonstrated rather than "
                "asserted"
            ),
        )
        self.assertEqual(counters[sarif.COUNTER_RULE_INDEX_UNUSABLE], 1)
        self.assertEqual(counters[sarif.COUNTER_RULE_ID_FROM_RULE_ID], 2)
        self.assertEqual(
            counters[sarif.COUNTER_RULE_METADATA_UNRESOLVED],
            0,
            msg=(
                "the rejected record returns before that counter is reached, and both "
                "emitted records resolved their rule object"
            ),
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

    def test_the_rules_default_configuration_level_is_not_consulted(self) -> None:
        """A rule's ``defaultConfiguration.level`` is not one of the authorised sources.

        AAP 0.5.4's per-shape table enumerates the field sources for a shared-SARIF
        record as the result's own ``level``, then the rule's ``properties.severity`` or
        ``properties.problem.severity``.  ``rule.defaultConfiguration.level`` is not
        among them, so a result that states no level of its own and whose rule carries no
        properties severity has no stated severity at all -- however plainly the rule
        configures a default, and however routinely other SARIF consumers derive one
        from it.

        The assertion is the negative because that is the only thing that can catch the
        source being reintroduced: a rule configuring ``warning`` here must **not**
        produce ``severity_native`` ``warning`` or the Medium band, and the row must land
        on the ``no_vocabulary`` basis with ``severity_absent`` counted.  A test that
        merely checked the three authorised sources would pass either way.

        The counter for the removed source is asserted absent from the adapter's counter
        set as well, since a key left behind would keep appearing in every expected file
        and in ``normalize-run.json`` while nothing could ever move it.
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
        self.assertIsNone(
            rows[0]["severity_native"],
            msg=(
                "the rule's defaultConfiguration.level must not reach severity_native: "
                "AAP 0.5.4 enumerates the field sources for this shape and does not "
                "carry it"
            ),
        )
        self.assertNotEqual(rows[0]["severity_native"], "warning")
        self.assertEqual(rows[0]["severity_norm"], severity.INFO)
        self.assertNotEqual(rows[0]["severity_norm"], "Medium")
        self.assertEqual(tally.results[0].basis, severity.BASIS_NO_VOCABULARY)
        self.assertEqual(counters[sarif.COUNTER_SEVERITY_ABSENT], 1)
        self.assertEqual(counters[sarif.COUNTER_SEVERITY_FROM_LEVEL], 0)
        self.assertEqual(counters[sarif.COUNTER_SEVERITY_FROM_RULE_PROPERTY], 0)
        self.assertNotIn(
            "severity_from_rule_default_configuration",
            counters,
            msg=(
                "the counter for the removed source must be gone from the adapter's "
                "counter set, not left at a permanent zero"
            ),
        )
        self.assertNotIn(
            "severity_from_rule_default_configuration",
            sarif.COUNTER_KEYS,
            msg="and it must be gone from the authored counter vocabulary",
        )

    def test_a_rule_property_severity_is_the_second_source(self) -> None:
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

    def test_a_rule_problem_severity_is_the_third_source(self) -> None:
        """``properties.problem.severity`` is consulted where the two before it are silent."""
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

    def test_all_three_sources_silent_states_the_absence(self) -> None:
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

    def test_a_committed_fixture_exercises_the_multi_valued_counter(self) -> None:
        """The multi-valued path is reached from a committed fixture, not only in memory.

        The authored documents above pin the ordering; this checks that the same counter
        moves on a fixture committed to this tree, so the behaviour is not reachable only
        from input a test method wrote itself. Which fixture carries such a record is
        asserted from its own expected file -- the fixture is located by scanning the
        inventory rather than named here, and no property is attributed to a producer.

        The inventory is :data:`ROW_FIXTURES` rather than the captures alone. Measured
        across the three raw artifacts, not one of their 2002, 2126 and 1093 rules carries
        more than one distinct CVE identifier, so no captured excerpt can move this
        counter, and ``derived-semgrep-features`` carries the case with its provenance
        declared. Naming the captures here would force the assertion to be weakened to
        stay green; scanning the row-producing inventory for the fixture that records the
        case keeps it at full strength.
        """
        carrying = [
            (stem, tool, expectation)
            for stem, tool in ROW_FIXTURES.items()
            for expectation in (_read_json(_expected_path(stem)),)
            if expectation["counters"][sarif.COUNTER_MULTI_VALUED_CVE] > 0
        ]
        self.assertTrue(
            carrying,
            msg=(
                "no committed fixture records a multi-valued CVE any more, so the ordering "
                "rule is exercised only by documents authored inside a test method"
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
        for stem, tool in ROW_FIXTURES.items():
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
# Percent-decoded control characters (F16). The control test in
# ``paths.parse_uri_reference`` is made twice, and this class is the assertion that the
# *second* one exists: ``%1b`` is three ordinary URI characters as written and becomes
# ESC only once ``unquote`` has run, so a check made on the raw reference alone passes
# every hostile reference here. The decoded value is what becomes the row's ``path``, and
# from there it reaches ``findings.json``, the ``path`` column of ``findings.csv``, the
# Markdown records rendered from them and the run logs -- CWE-176 for the decode that is
# not revalidated, CWE-117 for where the decoded value arrives.
#
# Measured over the committed dataset before the guard was written: of 9,427 rows, none
# carries a control character in ``path`` and none carries a ``%`` either, so no
# percent-encoded sequence exists in this provisioning's output for the guard to decode
# or refuse. That is why every case below is authored or derived, and why the guard's
# correctness cannot rest on the captured artifacts alone.
# --------------------------------------------------------------------------------------


class PercentEncodedControlTests(SarifAdapterTestCase):
    """A reference that is a valid URI as written and is not one once decoded.

    Two guards are asserted here, and they are different moments rather than one check
    written twice. The first is at the decode: every ``unquote`` in
    :func:`normalize.paths.parse_uri_reference` is followed by a control test, and a
    reference that fails it yields the ``invalid`` form, which the adapter rejects under
    ``invalid_uri``. The second is immediately before a canonical path is emitted:
    :func:`normalize.paths.assert_relative_path` refuses a control-bearing path outright,
    and :func:`normalize.paths._emitted_path_or_refusal` turns that refusal into a counted
    rejection for the resolvers that carry artifact content.

    The order matters and is asserted, not assumed: refusing at the decode names the
    cause -- which decode produced the control, and which code point -- while refusing at
    the emission names only the symptom. A SARIF reference is always stopped at the
    decode, so the second guard is reached here only through the two functions directly.
    """

    #: The three code points the fixture and these cases use, with the reason each is in
    #: the set. NUL truncates a C string and is the classic filename-smuggling character;
    #: CR splits one log line into two, one of them attacker-composed; ESC opens a
    #: terminal escape sequence in anything that renders the record. Each is a control
    #: character under RFC 3986, which is what makes the reference invalid rather than
    #: merely unusual, and none of the three appears anywhere in the 4,095 in-scope files.
    CONTROL_CASES: tuple[tuple[str, str, str], ...] = (
        ("%00", "\u0000", "U+0000"),
        ("%0D", "\r", "U+000D"),
        ("%1B", "\u001b", "U+001B"),
    )

    #: The fixture stem this class holds to its expectation.
    FIXTURE = "reject-sarif-percent-encoded-control"

    def _adapt_uri(
        self, uri: str, **result_kwargs: Any
    ) -> tuple[list[dict[str, Any]], list[paths.Rejection]]:
        """Adapt a one-result document addressing ``uri`` and return rows and rejections."""
        document = authored_document([authored_result(uri, **result_kwargs)])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        return rows, rejections

    def test_every_decode_site_refuses_a_percent_encoded_control(self) -> None:
        """One case per ``unquote`` in the parser, each named by the detail it produces.

        Six sites, because a guard placed on one decode leaves the others open and the
        branches do not share a code path: a relative reference is decoded whole, a
        ``file:`` URI has only its path component decoded after ``urlsplit`` has parsed
        it, an archive reference decodes its container and its member separately, and a
        reference carrying an archive scheme parses its container recursively -- so the
        container's own decode is a seventh opportunity reached through a different frame.
        The committed fixture exercises three of these plus the base-map walk; the rest are
        authored here rather than left to a fixture that would carry no further assertion.

        Each detail is asserted to name the decode site, so a future change that refused
        every reference for one reason would fail here rather than pass six times over.
        """
        cases = {
            "relative reference": (
                "core/src/main/scala/org/apache/spark/storage/DiskStore%1B.scala",
                "the URI reference decodes to a value",
                "U+001B",
            ),
            "file URI path": (
                "file:///opt/spark-src/core/src/main/scala/A%00.scala",
                "the path of the file URI decodes to a value",
                "U+0000",
            ),
            "archive member, no scheme": (
                "core/target/authored.jar!/org/apache/spark/A%0D.class",
                "the member of the archive reference decodes to a value",
                "U+000D",
            ),
            "archive container, no scheme": (
                "core/target/authored%0D.jar!/org/apache/spark/A.class",
                "the container of the archive reference is invalid",
                "U+000D",
            ),
            "archive member, jar scheme": (
                "jar:file:///opt/spark-src/core/target/authored.jar!/org/apache/%1BA.class",
                "the member of the 'jar' URI decodes to a value",
                "U+001B",
            ),
            "archive container, jar scheme": (
                "jar:file:///opt/spark-src/core/target/authored%0A.jar!/org/apache/A.class",
                "the container of the 'jar' URI is invalid",
                "U+000A",
            ),
        }
        for label, (uri, site_phrase, code_point) in cases.items():
            with self.subTest(decode_site=label):
                self.assertIsNone(
                    paths.describe_control_characters(uri),
                    msg=(
                        f"{label}: the authored reference must carry no control character "
                        "as written, or it would be refused by the pre-decode check and "
                        "would prove nothing about the post-decode one"
                    ),
                )
                rows, rejections = self._adapt_uri(uri)
                self.assertEqual(rows, [], msg=f"{label}: no row may be emitted")
                rejection = self.assert_single_rejection(
                    rejections,
                    reject_class=paths.REJECT_INVALID_URI,
                    label=f"authored {label}",
                )
                detail = rejection.detail
                self.assertIn(
                    site_phrase,
                    detail,
                    msg=(
                        f"{label}: the detail no longer names which decode produced the "
                        "control, so a reader could not tell this site from its siblings"
                    ),
                )
                self.assertIn(
                    code_point,
                    detail,
                    msg=f"{label}: the detail no longer names the decoded control",
                )

    def test_a_control_bearing_base_map_entry_is_refused_rather_than_joined(self) -> None:
        """The record's own reference is clean; the base it names is not.

        A guard applied only to the result's ``uri`` would emit this row with a control
        character joined into the middle of its path out of the base map, which is why the
        walk validates each entry's own ``uri`` as a URI reference. The outcome is
        ``invalid-uri`` rather than ``degenerate``, so it is *not* eligible for the
        runner-recorded fallback AAP 0.5.4 allows a degenerate base: an entry whose ``uri``
        is not a URI reference is malformed rather than merely useless, and falling back
        would resolve a record whose own base map contradicts itself. The metadata here
        does record an explicit base for ``opengrep``, so the fallback was available and
        was correctly not taken.
        """
        document = authored_document(
            [authored_result("core/src/main/scala/authored.scala", uri_base_id="%ESC%")],
            base_map={"%ESC%": {"uri": "file:///opt/spark-src%1B/"}},
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        rejection = self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_INVALID_URI,
            label="authored control-bearing base entry",
        )
        detail = rejection.detail
        self.assertIn(
            "of the entry for '%ESC%'",
            detail,
            msg=(
                "the detail must name the base entry that is malformed rather than the "
                "record's own clean uri, or it would send a reader to the wrong place"
            ),
        )
        self.assertIn("U+001B", detail)

    def test_a_benign_percent_encoded_reference_still_becomes_a_row(self) -> None:
        """Percent-encoding is not itself suspicious, and is not what is refused.

        A guard that refused every ``%`` would satisfy every rejection assertion in this
        class and would be wrong: ``%44`` is ``D`` and ``%20`` is a space, both legal in a
        path, and a reference carrying them names a real location. The errata behaviours
        survive the decode too, which is asserted in the same place because the decode is
        where they would most easily be lost: ``%2E%2E`` decodes to ``..`` and the segments
        are kept rather than normalized away (SARIF 2.1.0 errata, the section 3.10.2
        amendment).
        """
        cases = {
            "an encoded letter decodes to the captured path": (
                "core/src/main/scala/org/apache/spark/storage/%44iskStore.scala",
                DISK_STORE_PATH,
                True,
            ),
            "an encoded space is kept in the emitted path": (
                "core/src/main/scala/org/apache/spark/storage/Disk%20Store.scala",
                "core/src/main/scala/org/apache/spark/storage/Disk Store.scala",
                True,
            ),
            "encoded dot-dot segments are preserved": (
                "core/src/main/scala/%2E%2E/authored.scala",
                "core/src/main/scala/../authored.scala",
                True,
            ),
        }
        for label, (uri, expected_path, expected_in_scope) in cases.items():
            with self.subTest(case=label):
                rows, rejections = self._adapt_uri(uri)
                self.assertEqual(rejections, [], msg=f"{label}: nothing may be rejected")
                self.assertEqual(len(rows), 1)
                self.assertEqual(
                    rows[0]["path"],
                    expected_path,
                    msg=f"{label}: the decoded path is not the one the reference names",
                )
                self.assertIs(rows[0]["in_scope"], expected_in_scope)

    def test_a_literal_control_is_still_refused_before_any_decode(self) -> None:
        """The original pre-decode check is intact, and its diagnosis is still its own.

        The second guard is an addition rather than a replacement: a reference carrying a
        literal control character is still refused during parsing, and its detail still
        says the reference *carries* a control rather than that it decodes to one -- so the
        two conditions stay distinguishable in the record, which is what a reader needs to
        tell a producer emitting a raw control from one emitting an encoded one.
        """
        rows, rejections = self._adapt_uri("core/src/main/scala/A\u001b.scala")
        self.assertEqual(rows, [])
        rejection = self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_INVALID_URI,
            label="authored literal control",
        )
        detail = rejection.detail
        self.assertIn("carries a control character", detail)
        self.assertNotIn(
            "decodes to a value",
            detail,
            msg=(
                "a literal control is not a decode failure, and recording it as one would "
                "misattribute the producer's shape"
            ),
        )

    def test_the_pre_emission_guard_refuses_a_control_bearing_path_with_no_decode_involved(
        self,
    ) -> None:
        """The second guard, exercised directly, because no SARIF document can reach it.

        A SARIF reference carrying a control is stopped at the decode one step earlier, so
        the pre-emission guard is reached only by the resolvers that take a tool's own
        reported path field -- which other adapters own. The invariant is ``paths``'s and is
        shared by every adapter, so it is asserted here against the two functions rather
        than left to a document that cannot reach it.

        Both halves are asserted, because they are deliberately different outcomes.
        :func:`normalize.paths.assert_relative_path` **raises**, which is the backstop no
        future resolver can bypass -- :meth:`normalize.paths.ResolvedPath.__post_init__`
        calls it, so every construction runs through it, including the
        ``<container>!<member>`` archive serialization.
        :func:`normalize.paths._emitted_path_or_refusal` **returns** a counted rejection,
        which is the right outcome for artifact content: an escaped ``PathPolicyError`` is
        a whole-artifact fault in ``cli.py``, so a raise here would let one hostile record
        deny the several thousand parsable ones beside it (AAP 0.5.4's partial parse).
        """
        hostile = "core/src/main/scala/A\u001b.scala"
        with self.assertRaises(paths.PathPolicyError) as raised:
            paths.assert_relative_path(hostile)
        message = str(raised.exception)
        self.assertIn("U+001B", message)
        self.assertNotIn(
            "\u001b",
            message,
            msg="the refusal must describe the control, never reproduce it",
        )
        with self.assertRaises(paths.PathPolicyError):
            paths.ResolvedPath(
                path=hostile,
                kind=paths.PATH_KIND_TREE_FILE,
                basis=paths.BASIS_ALREADY_ROOT_RELATIVE,
                tool="opengrep",
            )
        with self.assertRaises(paths.PathPolicyError):
            # The archive serialization reaches the same backstop through construction,
            # which is why no separate check is written into archive_member_path.
            paths.archive_member_path(
                "core/target/authored.jar",
                "org/apache/spark/A\u001b.class",
                root=self.scan_root,
                tool="opengrep",
            )
        refusal = paths._emitted_path_or_refusal(
            hostile,
            kind=paths.PATH_KIND_TREE_FILE,
            basis=paths.BASIS_ALREADY_ROOT_RELATIVE,
            tool="opengrep",
            reject_class=paths.REJECT_MALFORMED_RECORD,
            identity={"authored": "pre-emission guard"},
            context="the authored path",
        )
        self.assertIsInstance(
            refusal,
            paths.Rejection,
            msg="artifact content earns a counted rejection rather than a raise",
        )
        self.assertEqual(refusal.reject_class, paths.REJECT_MALFORMED_RECORD)
        self.assertIn("U+001B", refusal.detail)
        self.assertNotIn("\u001b", refusal.detail)
        self.assertEqual(dict(refusal.record_identity), {"authored": "pre-emission guard"})
        emitted = paths._emitted_path_or_refusal(
            DISK_STORE_PATH,
            kind=paths.PATH_KIND_TREE_FILE,
            basis=paths.BASIS_ALREADY_ROOT_RELATIVE,
            tool="opengrep",
            reject_class=paths.REJECT_MALFORMED_RECORD,
            identity={"authored": "pre-emission guard"},
            context="the authored path",
        )
        self.assertIsInstance(
            emitted,
            paths.ResolvedPath,
            msg="an ordinary path is unaffected by the guard",
        )
        self.assertEqual(emitted.path, DISK_STORE_PATH)

    def test_the_control_describer_names_code_points_and_never_reproduces_them(self) -> None:
        """The describer is what keeps every one of these diagnostics safe to persist.

        Every detail above is composed from it, and the details are written verbatim into
        ``harness/artifacts/logs/`` and ``normalize-run.json``, so a describer that echoed
        the character would carry ESC or CR into a log line -- the CWE-117 half of the
        finding, reached through the very record that refused the value. ``repr`` would
        escape it too, but only incidentally, and a safety property must not rest on a
        formatting choice.

        The cap is asserted for the same reason: a hostile value carrying a thousand
        controls would otherwise turn one detail into a wall of text in a preserved log.
        """
        self.assertIsNone(
            paths.describe_control_characters(DISK_STORE_PATH),
            msg="an ordinary path carries nothing to describe",
        )
        for encoded, character, code_point in self.CONTROL_CASES:
            with self.subTest(control=code_point):
                described = paths.describe_control_characters(f"a{character}b")
                self.assertIsNotNone(described)
                assert described is not None  # for the type checker; asserted above
                self.assertIn(code_point, described)
                self.assertIn("at index 1", described)
                self.assertNotIn(character, described)
                self.assertNotIn(encoded.lower(), described.lower())
        many = "a\u0000b\r c\u001bd\u0007e"
        described = paths.describe_control_characters(many)
        self.assertIsNotNone(described)
        assert described is not None  # for the type checker; asserted above
        self.assertIn("4 control characters", described)
        self.assertIn("the first 3 being", described)
        for character in ("\u0000", "\r", "\u001b", "\u0007"):
            self.assertNotIn(character, described)

    def test_no_rejection_detail_carries_a_control_character(self) -> None:
        """Over every negative fixture, not only this one.

        The property is a pipeline-wide one: a rejection's detail and record identity are
        persisted, so a control reaching either is log injection whichever fixture produced
        it. Row paths are checked in the same pass, which is the F16 outcome stated
        positively -- no emitted path carries a control character.
        """
        for stem in NEGATIVE_FIXTURES:
            expectation = _read_json(_expected_path(stem))
            if "branches" in expectation:
                kind = expectation["branches"][0]["precondition"]["tool_path_base"]["kind"]
            else:
                kind = expected_path_base_kind(expectation)
            tool = negative_fixture_tool(stem)
            with self.subTest(fixture=stem):
                rows, rejections, _counters, _tally = self.adapt(
                    _read_json(_fixture_path(stem)),
                    tool=tool,
                    tool_base=self.base_of_kind(tool, kind),
                )
                for index, rejection in enumerate(rejections):
                    self.assertIsNone(
                        paths.describe_control_characters(rejection.detail),
                        msg=(
                            f"{stem} rejection {index}: the detail carries a control "
                            "character, which the preserved logs would carry with it"
                        ),
                    )
                    for key, value in rejection.record_identity.items():
                        if isinstance(value, str):
                            self.assertIsNone(
                                paths.describe_control_characters(value),
                                msg=(
                                    f"{stem} rejection {index}: the identity's {key!r} "
                                    "carries a control character"
                                ),
                            )
                for index, row in enumerate(rows):
                    self.assertIsNone(
                        paths.describe_control_characters(row["path"]),
                        msg=(
                            f"{stem} row {index}: an emitted path carries a control "
                            "character, which is the F16 outcome itself"
                        ),
                    )

    def test_the_percent_encoded_control_fixture_rejects_only_its_defective_records(
        self,
    ) -> None:
        """The fixture, held to its expectation and to the partial-parse boundary.

        Four records are refused and two are emitted, and the two straddle the four: the
        first element of the array and the last both become rows, so a run that abandoned
        the artifact at the first control could not have produced the second row. The
        emitted rows are the same two captured records the sibling ``uriBaseId`` fixture
        carries, which is what makes the pair falsifiable -- the same rules, the same
        messages and the same lines reach rows there and rejections here, and the
        ``artifactLocation`` is the only difference.
        """
        expectation = _read_json(_expected_path(self.FIXTURE))
        rows, rejections, counters, _tally = self.adapt_fixture(
            self.FIXTURE,
            tool="opengrep",
            tool_base=self.base_of_kind("opengrep", expected_path_base_kind(expectation)),
        )
        self.assert_rows_match(rows, expectation["rows"], label=self.FIXTURE)
        self.assert_schema_invariants(rows, label=self.FIXTURE)
        self.assertEqual(
            [row["start_line"] for row in rows],
            [DISK_STORE_LINE, 75],
            msg=(
                "the surviving rows must be the first and last records, which is what "
                "shows the traversal continued past all four rejections"
            ),
        )
        self.assertEqual(len(rejections), 4)
        self.assertEqual(
            [rejection.reject_class for rejection in rejections],
            [paths.REJECT_INVALID_URI] * 4,
            msg="all four are the same class and are told apart by their details",
        )
        self.assertEqual(
            [rejection.record_identity["result_index"] for rejection in rejections],
            [1, 2, 3, 4],
            msg="the rejections name the four defective records in document order",
        )
        distinct_sites = {
            phrase
            for phrase in (
                "the URI reference decodes to a value",
                "the path of the file URI decodes to a value",
                "the member of the archive reference decodes to a value",
                "of the entry for '%ESCROOT%'",
            )
            if any(phrase in rejection.detail for rejection in rejections)
        }
        self.assertEqual(
            len(distinct_sites),
            4,
            msg=(
                "the fixture must exercise four distinct decode sites, or one of its "
                "records is a duplicate of another and carries no further assertion"
            ),
        )
        self.assert_counters_match(counters, expectation["counters"], label=self.FIXTURE)
        self.assertEqual(
            counters[sarif.COUNTER_RULE_ID_FROM_RULE_ID],
            6,
            msg=(
                "every record resolves its identifier at step 2 and four are refused at "
                "step 4, so the identifier counter counts six rather than two -- the "
                "classification order made visible"
            ),
        )
        self.assertEqual(
            counters[f"{sarif.COUNTER_PATH_KIND_PREFIX}{paths.PATH_KIND_ARCHIVE_MEMBER}"],
            0,
        )
        self.assertEqual(
            counters[sarif.COUNTER_NON_FILESYSTEM_PATHS],
            0,
            msg=(
                "the archive reference is refused while it is still being parsed, before "
                "a form is assigned, so it is never classified as a non-filesystem path"
            ),
        )
        self.assert_reconciliation_identity(
            _read_json(_fixture_path(self.FIXTURE)),
            rows,
            rejections,
            label=self.FIXTURE,
            expected_records=6,
        )


# --------------------------------------------------------------------------------------
# The other half of the same guard: a '%' that is not the start of an escape at all.
# --------------------------------------------------------------------------------------


class MalformedPercentEscapeTests(SarifAdapterTestCase):
    """A reference whose ``%`` is not the start of a two-hexadecimal-digit escape.

    The counterpart to :class:`PercentEncodedControlTests`, and the reason the two are
    separate classes is that they refuse a reference at two different moments for two
    different reasons. That class covers a *well-formed* escape whose decoded value is
    inadmissible; this one covers a reference that does not decode at all, because
    :func:`urllib.parse.unquote` is documented to leave an invalid escape **in place**
    rather than raise.

    That documented tolerance is the whole defect (SEC-06). A ``%``, a ``%2`` or a
    ``%GG`` survives ``unquote`` unchanged and then passes every downstream test there
    is: the value is a non-empty string, it carries no control character, it relativizes
    against the recorded scan root, and it matches an allowlist glob. What arrives in the
    dataset is a ``path`` column entry that is not a path, with an ``in_scope`` verdict
    computed from it, indistinguishable in ``findings.json`` from a real file. So the
    refusal cannot be a decode-time check -- there is nothing for a decode-time check to
    see -- and has to be a syntax check made **before** any decode.

    Four properties are asserted here and each is a different way the fix could be wrong:
    that every guard site refuses a malformed escape and names its own component; that a
    *well-formed* escape is still decoded and still becomes a row, since a guard refusing
    every ``%`` would satisfy every rejection assertion in this class and be wrong; that
    ``%00`` still earns the control-character diagnosis rather than this one, which is
    what keeps the two conditions distinguishable in the record; and that a percent-wrapped
    ``uriBaseId`` **identifier** is a mapping key rather than a URI reference, which is
    the one regression that would break SARIF path resolution silently.
    """

    #: The fixture stem this class holds to its expectation.
    FIXTURE = "reject-sarif-malformed-percent-escape"

    #: The two fault kinds, as :func:`normalize.paths.describe_malformed_percent_escapes`
    #: words them. Held as constants because every assertion below compares against the
    #: describer's own phrasing rather than against a paraphrase of it.
    FAULT_TRUNCATED = "'%' is followed by fewer than two characters"
    FAULT_NON_HEX = "'%' is not followed by two hexadecimal digits"

    def _adapt_uri(
        self, uri: str, **result_kwargs: Any
    ) -> tuple[list[dict[str, Any]], list[paths.Rejection]]:
        """Adapt a one-result document addressing ``uri`` and return rows and rejections."""
        document = authored_document([authored_result(uri, **result_kwargs)])
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        return rows, rejections

    def test_the_qa_reproduction_no_longer_reaches_a_row(self) -> None:
        """The three references the finding names, each now a counted rejection.

        This is the finding's own reproduction, asserted rather than described: ``%``,
        ``%2`` and ``%GG`` returned the ``relative`` form and reached the dataset, one of
        them with ``in_scope`` true. Each is now the ``invalid`` form, and the adapter
        counts it under ``invalid_uri``.

        The bare forms are asserted against the parser and the embedded forms against the
        adapter, because the two answer different questions: the parser's ``form`` is what
        every caller branches on, and the adapter's rejection is what the dataset records.
        """
        bare = {
            "a bare per-cent sign": ("%", self.FAULT_TRUNCATED, 0),
            "an escape cut short": ("%2", self.FAULT_TRUNCATED, 0),
            "two characters that are not hex digits": ("%GG", self.FAULT_NON_HEX, 0),
        }
        for label, (raw, fault, index) in bare.items():
            with self.subTest(bare=label):
                reference = paths.parse_uri_reference(raw)
                self.assertEqual(
                    reference.form,
                    paths.URI_FORM_INVALID,
                    msg=(
                        f"{label}: {raw!r} is not a URI reference, and any form other than "
                        "invalid puts it on a route towards a row"
                    ),
                )
                self.assertIsNotNone(reference.detail)
                assert reference.detail is not None  # for the type checker
                self.assertIn(fault, reference.detail)
                self.assertIn(f"at index {index}", reference.detail)

        embedded = {
            "inside a relative path": (
                "core/src/main/scala/org/apache/spark/storage/DiskStore%GG.scala",
                self.FAULT_NON_HEX,
            ),
            "at the end of a relative path": (
                "core/src/main/scala/org/apache/spark/storage/DiskStore.scala%",
                self.FAULT_TRUNCATED,
            ),
            "cut short before an extension": (
                "core/src/main/scala/org/apache/spark/storage/DiskStore%2.scala",
                self.FAULT_NON_HEX,
            ),
        }
        for label, (uri, fault) in embedded.items():
            with self.subTest(embedded=label):
                rows, rejections = self._adapt_uri(uri)
                self.assertEqual(
                    rows,
                    [],
                    msg=(
                        f"{label}: a reference that is not a path must not reach the path "
                        "column, whatever its in_scope verdict would have been"
                    ),
                )
                rejection = self.assert_single_rejection(
                    rejections,
                    reject_class=paths.REJECT_INVALID_URI,
                    label=f"authored {label}",
                )
                self.assertIn(fault, rejection.detail)

    def test_every_guard_site_refuses_a_malformed_escape(self) -> None:
        """One case per validation site, each named by the component its detail reports.

        The parser validates the whole raw reference at its head and then each component
        again immediately before that component's own ``unquote``. The head check already
        covers every component -- each one is a substring of the raw reference -- so these
        cases are asserted through the parser's own ``detail`` and the point of them is
        that no *route* through the function reaches a decode with escapes that are not
        escapes. A reference of each shape is authored rather than left to the fixture,
        which carries four of them and cannot carry all of them without repeating itself.
        """
        cases = {
            "relative reference": (
                "core/src/main/scala/org/apache/spark/storage/DiskStore%G0.scala",
                self.FAULT_NON_HEX,
            ),
            "file URI path": (
                "file:///opt/spark-src/core/src/main/scala/A%2.scala",
                self.FAULT_NON_HEX,
            ),
            "archive member, no scheme": (
                "core/target/authored.jar!/org/apache/spark/A%ZZ.class",
                self.FAULT_NON_HEX,
            ),
            "archive container, no scheme": (
                "core/target/authored%.jar!/org/apache/spark/A.class",
                self.FAULT_NON_HEX,
            ),
            "archive member, jar scheme": (
                "jar:file:///opt/spark-src/core/target/authored.jar!/org/apache/A%1.class",
                self.FAULT_NON_HEX,
            ),
            "archive container, jar scheme": (
                "jar:file:///opt/spark-src/core/target/authored%QQ.jar!/org/apache/A.class",
                self.FAULT_NON_HEX,
            ),
            "foreign scheme": ("http://example.invalid/a%2", self.FAULT_TRUNCATED),
            "trailing per-cent at the very end": (
                "core/src/main/scala/A.scala%",
                self.FAULT_TRUNCATED,
            ),
        }
        for label, (uri, fault) in cases.items():
            with self.subTest(guard_site=label):
                self.assertIsNone(
                    paths.describe_control_characters(uri),
                    msg=(
                        f"{label}: the authored reference must carry no control character, "
                        "or it would be refused by the control check and would prove "
                        "nothing about the percent check"
                    ),
                )
                reference = paths.parse_uri_reference(uri)
                self.assertEqual(
                    reference.form,
                    paths.URI_FORM_INVALID,
                    msg=f"{label}: the reference must be classified invalid",
                )
                detail = reference.detail or ""
                self.assertIn(fault, detail, msg=f"{label}: the fault kind is not named")
                self.assertIn(
                    "RFC 3986 section 2.1",
                    detail,
                    msg=(
                        f"{label}: the detail must say which production was violated, so a "
                        "reader can check the refusal against the specification"
                    ),
                )
                self.assertNotIn(
                    uri,
                    detail,
                    msg=(
                        f"{label}: the detail must not reproduce the reference it refused "
                        "-- it is written verbatim into the preserved logs (SEC-04)"
                    ),
                )

    def test_the_foreign_scheme_reclassification_is_recorded_rather_than_silent(self) -> None:
        """``http://h/%2`` moves from ``unresolvable_path`` to ``invalid_uri``.

        A behaviour change, asserted so it cannot happen unnoticed. Before the syntax
        guard a foreign-scheme reference was returned undecoded and unchecked, so it
        reached ``foreign-scheme`` and the adapter rejected it under ``unresolvable_path``
        -- syntactically valid, just not a location in the tree. It is now refused at the
        head of the function instead, because it is not syntactically valid at all.

        Both outcomes are rejections and neither ever produced a row, so the dataset is
        unaffected; what changes is which class the record is counted under, and
        ``invalid_uri`` is the more precise of the two. A well-formed foreign-scheme
        reference is asserted alongside, unchanged, so the reclassification is shown to
        be about the escape rather than about the scheme.
        """
        malformed = paths.parse_uri_reference("http://example.invalid/a%2")
        self.assertEqual(malformed.form, paths.URI_FORM_INVALID)
        self.assertIn(self.FAULT_TRUNCATED, malformed.detail or "")
        rows, rejections = self._adapt_uri("http://example.invalid/a%2")
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_INVALID_URI,
            label="authored malformed foreign-scheme reference",
        )

        wellformed = paths.parse_uri_reference("http://example.invalid/a%20b")
        self.assertEqual(
            wellformed.form,
            paths.URI_FORM_FOREIGN_SCHEME,
            msg=(
                "a foreign-scheme reference whose escapes are well formed is still "
                "foreign-scheme, so the reclassification is about the escape and not the "
                "scheme"
            ),
        )
        rows, rejections = self._adapt_uri("http://example.invalid/a%20b")
        self.assertEqual(rows, [])
        self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_UNRESOLVABLE_PATH,
            label="authored well-formed foreign-scheme reference",
        )

    def test_a_well_formed_escape_still_decodes_and_still_becomes_a_row(self) -> None:
        """The control that keeps the guard from being a blanket refusal of ``%``.

        A guard that rejected every reference containing a ``%`` would satisfy every
        rejection assertion in this class and would be wrong twice over: it would lose
        real rows, and it would lose them silently, since a rejected record leaves no row
        to notice. Each case below is a well-formed escape whose decoded value is a legal
        path character, and each must still reach the dataset with the decoded path.

        The errata behaviours are asserted in the same place because the syntax check runs
        before the decode and a check that mis-parsed ``%2E`` would take them with it: the
        SARIF 2.1.0 errata section 3.10.2 amendment requires ``..`` segments to be kept
        rather than normalized away.
        """
        cases = {
            "an encoded letter decodes to the captured path": (
                "core/src/main/scala/org/apache/spark/storage/%44iskStore.scala",
                DISK_STORE_PATH,
            ),
            "an encoded upper-case letter mid-name": (
                "resource-managers/yarn/src/main/java/org/apache/spark/deploy/yarn/"
                "Proxy%55tils.java",
                "resource-managers/yarn/src/main/java/org/apache/spark/deploy/yarn/"
                "ProxyUtils.java",
            ),
            "an encoded space is kept in the emitted path": (
                "core/src/main/scala/org/apache/spark/storage/Disk%20Store.scala",
                "core/src/main/scala/org/apache/spark/storage/Disk Store.scala",
            ),
            "an encoded per-cent sign decodes to one": (
                "core/src/main/scala/org/apache/spark/storage/Disk%25Store.scala",
                "core/src/main/scala/org/apache/spark/storage/Disk%Store.scala",
            ),
            "lower-case hex digits are accepted": (
                "core/src/main/scala/org/apache/spark/storage/Disk%2dStore.scala",
                "core/src/main/scala/org/apache/spark/storage/Disk-Store.scala",
            ),
            "encoded dot-dot segments are preserved": (
                "core/src/main/scala/%2E%2E/authored.scala",
                "core/src/main/scala/../authored.scala",
            ),
        }
        for label, (uri, expected_path) in cases.items():
            with self.subTest(case=label):
                self.assertIsNone(
                    paths.describe_malformed_percent_escapes(uri),
                    msg=f"{label}: the authored reference must be well formed as written",
                )
                rows, rejections = self._adapt_uri(uri)
                self.assertEqual(rejections, [], msg=f"{label}: nothing may be rejected")
                self.assertEqual(len(rows), 1)
                self.assertEqual(
                    rows[0]["path"],
                    expected_path,
                    msg=f"{label}: the decoded path is not the one the reference names",
                )

    def test_a_well_formed_escape_that_decodes_to_a_control_keeps_its_own_diagnosis(
        self,
    ) -> None:
        """``%00`` is refused, and refused for the reason it always was.

        The ordering property, asserted rather than assumed. ``%00`` is a *well-formed*
        escape, so the syntax check passes it and the control check refuses it a moment
        later on the decoded NUL. Had the two guards been written in the other order, or
        had the syntax check been written to reject anything it could not turn into a
        printable character, this record would carry the malformed-escape diagnosis and a
        reader could no longer tell a producer that emitted a broken escape from one that
        emitted a hostile well-formed escape.
        """
        for encoded, code_point in (("%00", "U+0000"), ("%0D", "U+000D"), ("%1B", "U+001B")):
            with self.subTest(escape=encoded):
                self.assertIsNone(
                    paths.describe_malformed_percent_escapes(encoded),
                    msg=f"{encoded} is a well-formed escape and must pass the syntax check",
                )
                reference = paths.parse_uri_reference(
                    f"core/src/main/scala/A{encoded}.scala"
                )
                self.assertEqual(reference.form, paths.URI_FORM_INVALID)
                detail = reference.detail or ""
                self.assertIn("decodes to a value that carries the control character", detail)
                self.assertIn(code_point, detail)
                self.assertNotIn(
                    "malformed percent escape",
                    detail,
                    msg=(
                        f"{encoded} is well formed, so recording it as a malformed escape "
                        "would misattribute the producer's shape"
                    ),
                )

    def test_a_percent_wrapped_uri_base_id_is_a_mapping_key_and_still_resolves(self) -> None:
        """The regression that would break SARIF path resolution silently.

        Every ``uriBaseId`` this repository's corpus carries is percent-wrapped --
        ``%SRCROOT%``, ``%PROJECTROOT%`` -- and read as a URI reference each one is a
        malformed escape. They are *mapping keys*: they index
        ``run.originalUriBaseIds`` and are never parsed as references, and no call site of
        :func:`normalize.paths.parse_uri_reference` passes one.

        Asserted in both directions. First that such an identifier *would* be refused as a
        reference, which is what makes the hazard real rather than hypothetical. Then that
        the walk resolves it anyway -- one level, a two-link chain and a three-link chain --
        and that a based reference still becomes a row. A guard applied to identifiers
        would not raise: every based reference would become ``invalid_uri`` and the dataset
        would quietly lose its rows.
        """
        for identifier in ("%SRCROOT%", "%PROJECTROOT%", "%PCTROOT%"):
            with self.subTest(identifier=identifier):
                self.assertEqual(
                    paths.parse_uri_reference(identifier).form,
                    paths.URI_FORM_INVALID,
                    msg=(
                        f"{identifier!r} is a malformed escape as a URI reference, which is "
                        "why it must never be read as one"
                    ),
                )

        one_level = paths.resolve_uri_base("%SRCROOT%", {"%SRCROOT%": {"uri": "file:///opt/spark-src/"}})
        self.assertEqual(one_level.outcome, paths.BASE_OUTCOME_RESOLVED)
        self.assertEqual(one_level.base, "/opt/spark-src")

        chained = paths.resolve_uri_base(
            "%SRCROOT%",
            {
                "%PROJECTROOT%": {"uri": "file:///opt/"},
                "%SRCROOT%": {"uri": "spark-src/", "uriBaseId": "%PROJECTROOT%"},
            },
        )
        self.assertEqual(chained.outcome, paths.BASE_OUTCOME_RESOLVED)
        self.assertEqual(chained.base, "/opt/spark-src")
        self.assertEqual(chained.chain, ("%SRCROOT%", "%PROJECTROOT%"))

        three_deep = paths.resolve_uri_base(
            "%C%",
            {
                "%A%": {"uri": "file:///opt/"},
                "%B%": {"uri": "spark-src/", "uriBaseId": "%A%"},
                "%C%": {"uri": "core/", "uriBaseId": "%B%"},
            },
        )
        self.assertEqual(three_deep.outcome, paths.BASE_OUTCOME_RESOLVED)
        self.assertEqual(three_deep.base, "/opt/spark-src/core")

        document = authored_document(
            [
                authored_result(
                    "core/src/main/scala/org/apache/spark/storage/DiskStore.scala",
                    uri_base_id="%SRCROOT%",
                )
            ],
            base_map={
                "%PROJECTROOT%": {"uri": "file:///opt/"},
                "%SRCROOT%": {"uri": "spark-src/", "uriBaseId": "%PROJECTROOT%"},
            },
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(
            rejections,
            [],
            msg=(
                "a based reference must still resolve; a percent guard reaching the "
                "identifier would reject every one of them"
            ),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], DISK_STORE_PATH)
        self.assertIs(rows[0]["in_scope"], True)

    def test_a_base_map_entry_with_a_malformed_uri_is_refused_rather_than_joined(self) -> None:
        """The record's own reference is clean; the base it names is not.

        A guard applied only to the result's ``uri`` would emit this row with a base joined
        into it that is not a path, which is why the walk validates each entry's own ``uri``
        as a URI reference. The outcome is ``invalid-uri`` rather than ``degenerate``, so it
        is not eligible for the runner-recorded fallback AAP 0.5.4 allows a degenerate base:
        an entry whose ``uri`` is not a URI reference is malformed rather than merely
        useless. The metadata here does record an explicit base for ``opengrep``, so the
        fallback was available and was correctly not taken.

        The detail names the base **identifier** and not the entry's ``uri`` value: the
        identifier is a mapping key this dataset publishes in its own ``record_identity``,
        while the value is what failed validation and is therefore described rather than
        shown (SEC-04).
        """
        document = authored_document(
            [authored_result("core/src/main/scala/authored.scala", uri_base_id="%PCT%")],
            base_map={"%PCT%": {"uri": "file:///opt/spark-src%GG/"}},
        )
        rows, rejections, _counters, _tally = self.adapt(document, tool="opengrep")
        self.assertEqual(rows, [])
        rejection = self.assert_single_rejection(
            rejections,
            reject_class=paths.REJECT_INVALID_URI,
            label="authored malformed base entry",
        )
        detail = rejection.detail
        self.assertIn(
            "of the entry for '%PCT%'",
            detail,
            msg=(
                "the detail must name the base entry that is malformed rather than the "
                "record's own clean uri, or it would send a reader to the wrong place"
            ),
        )
        self.assertIn(self.FAULT_NON_HEX, detail)
        self.assertNotIn(
            "file:///opt/spark-src%GG/",
            detail,
            msg="the entry's uri failed validation, so it is described and never published",
        )
        self.assertNotIn("%GG", detail)

    def test_the_escape_describer_names_positions_and_never_the_text(self) -> None:
        """The describer is what keeps every one of these diagnostics safe to persist.

        Every detail above is composed from it, and the details are written verbatim into
        ``harness/artifacts/logs/`` and ``normalize-run.json``, so a describer that echoed
        the offending characters would copy artifact bytes into a preserved record (SEC-04).
        It is one step stricter than
        :func:`normalize.paths.describe_control_characters` for a stated reason: a control
        character cannot carry a credential, so that describer may name its code point,
        while the two characters after a stray ``%`` can be any bytes at all.

        The cap is asserted for the same reason it is asserted there: a hostile reference
        carrying hundreds of stray ``%`` must not turn one rejection detail into a wall of
        text in a preserved log.
        """
        self.assertIsNone(
            paths.describe_malformed_percent_escapes(DISK_STORE_PATH),
            msg="a path with no '%' at all carries nothing to describe",
        )
        self.assertIsNone(
            paths.describe_malformed_percent_escapes("a%20b%2Fc%2e%2E"),
            msg="every escape here is well formed, in both hex cases",
        )
        self.assertIsNone(
            paths.describe_malformed_percent_escapes(42),  # type: ignore[arg-type]
            msg="a non-string is not a reference and is not described",
        )

        one = paths.describe_malformed_percent_escapes("abcSECRETVALUE%GG")
        self.assertIsNotNone(one)
        assert one is not None  # for the type checker; asserted above
        self.assertIn("the malformed percent escape", one)
        self.assertIn("at index 14", one)
        self.assertIn(self.FAULT_NON_HEX, one)
        self.assertNotIn("SECRETVALUE", one)
        self.assertNotIn("GG", one)

        two = paths.describe_malformed_percent_escapes("%2x%")
        self.assertIsNotNone(two)
        assert two is not None  # for the type checker; asserted above
        self.assertIn("2 malformed percent escapes", two)
        self.assertIn("at index 0", two)
        self.assertIn("at index 3", two)
        self.assertIn(self.FAULT_NON_HEX, two)
        self.assertIn(self.FAULT_TRUNCATED, two)

        many = paths.describe_malformed_percent_escapes("%z%z%z%z%z")
        self.assertIsNotNone(many)
        assert many is not None  # for the type checker; asserted above
        self.assertIn("5 malformed percent escapes", many)
        self.assertIn("the first 3 being", many)
        self.assertNotIn("at index 8", many)
        self.assertNotIn("z", many)

        # The index reported is an index into the ORIGINAL value, which only holds because
        # the well-formed triplets are masked with an equal-width sentinel rather than
        # removed. A describer that stripped them would report index 0 for this value.
        after_valid = paths.describe_malformed_percent_escapes("%20%20%GG")
        self.assertIsNotNone(after_valid)
        assert after_valid is not None  # for the type checker; asserted above
        self.assertIn("at index 6", after_valid)

    def test_the_fixture_rejects_only_its_defective_records(self) -> None:
        """The fixture, held to its expectation and to the partial-parse boundary.

        Four records are refused and two are emitted, and the two straddle the four: the
        first element of the array and the last both become rows, so a run that abandoned
        the artifact at the first fault could not have produced the second row. The last
        row is additionally the well-formed-escape control -- its reference reaches its path
        only by decoding ``%55`` -- so a blanket refusal of ``%`` would fail here rather
        than pass four times over.
        """
        expectation = _read_json(_expected_path(self.FIXTURE))
        rows, rejections, counters, _tally = self.adapt_fixture(
            self.FIXTURE,
            tool="opengrep",
            tool_base=self.base_of_kind("opengrep", expected_path_base_kind(expectation)),
        )
        self.assert_rows_match(rows, expectation["rows"], label=self.FIXTURE)
        self.assert_schema_invariants(rows, label=self.FIXTURE)
        self.assertEqual(
            [row["start_line"] for row in rows],
            [DISK_STORE_LINE, 75],
            msg=(
                "the surviving rows must be the first and last records, which is what "
                "shows the traversal continued past all four rejections"
            ),
        )
        self.assertEqual(
            rows[1]["path"],
            "resource-managers/yarn/src/main/java/org/apache/spark/deploy/yarn/"
            "ProxyUtils.java",
            msg=(
                "the last row's reference carries a well-formed %55, so its emitted path "
                "exists only if the escape was decoded rather than refused"
            ),
        )
        for index, row in enumerate(rows):
            self.assertNotIn(
                "%",
                row["path"],
                msg=(
                    f"row {index}: no emitted path may carry a per-cent sign in any form -- "
                    "measured over the committed dataset, none does"
                ),
            )
        self.assertEqual(len(rejections), 4)
        self.assertEqual(
            [rejection.reject_class for rejection in rejections],
            [paths.REJECT_INVALID_URI] * 4,
            msg="all four are the same class and are told apart by their details",
        )
        self.assertEqual(
            [rejection.record_identity["result_index"] for rejection in rejections],
            [1, 2, 3, 4],
            msg="the rejections name the four defective records in document order",
        )
        self.assertEqual(
            sum(1 for rejection in rejections if self.FAULT_TRUNCATED in rejection.detail),
            1,
            msg="exactly one record carries the truncated fault -- the trailing '%'",
        )
        self.assertEqual(
            sum(1 for rejection in rejections if self.FAULT_NON_HEX in rejection.detail),
            3,
            msg="the other three carry the non-hex fault",
        )
        reported_indices = {
            index
            for index in (56, 98, 88, 9)
            if any(f"at index {index}" in rejection.detail for rejection in rejections)
        }
        self.assertEqual(
            reported_indices,
            {56, 98, 88, 9},
            msg=(
                "each detail must name its own fault position, or two records are "
                "indistinguishable in the record"
            ),
        )
        self.assert_counters_match(counters, expectation["counters"], label=self.FIXTURE)
        self.assertEqual(
            counters[sarif.COUNTER_RULE_ID_FROM_RULE_ID],
            6,
            msg=(
                "every record resolves its identifier at step 2 and four are refused at "
                "step 4, so the identifier counter counts six rather than two"
            ),
        )
        self.assertEqual(
            counters[f"{sarif.COUNTER_PATH_KIND_PREFIX}{paths.PATH_KIND_ARCHIVE_MEMBER}"],
            0,
            msg=(
                "the archive reference is refused at the head of the parser, before the "
                "archive branch is reached, so no path kind is assigned to it"
            ),
        )
        self.assertEqual(counters[sarif.COUNTER_NON_FILESYSTEM_PATHS], 0)
        self.assert_reconciliation_identity(
            _read_json(_fixture_path(self.FIXTURE)),
            rows,
            rejections,
            label=self.FIXTURE,
            expected_records=6,
        )

    def test_no_rejection_detail_carries_the_reference_it_refused(self) -> None:
        """Over every negative fixture, not only this one.

        A rejection ``detail`` is persisted verbatim, so a detail that reproduced the value
        it refused would copy artifact bytes into ``harness/artifacts/logs/`` and
        ``normalize-run.json`` (SEC-04). The property is asserted from each expectation's
        own ``expected_detail_must_not_contain`` list, which is where each fixture records
        the specific strings a gate must have removed -- so a fixture that adds a condition
        adds its own assertion here rather than needing this test changed.
        """
        checked = 0
        for stem in NEGATIVE_FIXTURES:
            expectation = _read_json(_expected_path(stem))
            blocks = (
                [branch for branch in expectation["branches"]]
                if "branches" in expectation
                else [expectation]
            )
            for block in blocks:
                for rejection in block.get("rejections", ()):
                    for forbidden in rejection.get("expected_detail_must_not_contain", ()):
                        if not isinstance(forbidden, str):
                            continue
                        with self.subTest(fixture=stem, forbidden_length=len(forbidden)):
                            detail = rejection.get(
                                "expected_detail", rejection.get("detail", "")
                            )
                            self.assertNotIn(
                                forbidden,
                                detail,
                                msg=(
                                    f"{stem}: the recorded detail contains a string its own "
                                    "expectation records as having to be absent"
                                ),
                            )
                            checked += 1
        self.assertGreater(
            checked,
            0,
            msg=(
                "no expectation records a forbidden substring, so this test asserted "
                "nothing -- the corpus convention has been lost"
            ),
        )


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
        for stem in (*ROW_FIXTURES, *NEGATIVE_FIXTURES):
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
            for stem in (*ROW_FIXTURES, *NEGATIVE_FIXTURES)
            if not fixture_has_a_base_map(_read_json(_fixture_path(stem)))
        ]
        self.assertTrue(
            independent, msg="no fixture is root-independent, so this class asserts nothing"
        )
        for stem in independent:
            tool = ROW_FIXTURES.get(stem) or negative_fixture_tool(stem)
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
        tool: str = NEGATIVE_FIXTURE_TOOL,
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
        member is optional in a branch block and defaults to ``tool``, which the caller
        sets from :func:`negative_fixture_tool` so the default is this fixture's own
        producer rather than the module-wide one.
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
            expected.get("tool", tool),
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
        tool = negative_fixture_tool(stem)
        rows, rejections, produced_counters, _tally = self.adapt(
            document,
            tool=tool,
            tool_base=self.base_of_kind(tool, kind),
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
            self._assert_rejection(
                rejection, expected, label=f"{label} rejection {index}", tool=tool
            )
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
                # The expectation's own record of its producer, checked against the map the
                # loops adapt under. A fixture derived from another producer's output --
                # ``reject-sarif-rule-index-mismatch`` is the one -- must be declared in
                # both places, so neither can drift into adapting a fixture under a tool
                # its expected file does not name.
                self.assertEqual(
                    expectation["tool"],
                    negative_fixture_tool(stem),
                    msg=(
                        f"{stem}: the expected file records tool "
                        f"{expectation['tool']!r} while NEGATIVE_FIXTURE_TOOLS resolves "
                        f"{negative_fixture_tool(stem)!r}"
                    ),
                )
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
            tool = negative_fixture_tool(stem)
            for branch_id, kind in contexts:
                with self.subTest(fixture=stem, branch=branch_id):
                    rows, rejections, _counters, _tally = self.adapt(
                        document,
                        tool=tool,
                        tool_base=self.base_of_kind(tool, kind),
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
            tool = negative_fixture_tool(stem)
            with self.subTest(fixture=stem):
                rows, _rejections, _counters, tally = self.adapt_fixture(
                    stem,
                    tool=tool,
                    tool_base=self.base_of_kind(tool, kind),
                )
                self.assertEqual(
                    len(tally.results),
                    len(rows),
                    msg="the tally must be fed exactly once per emitted row",
                )
                self.assertEqual(
                    tally.delegate.row_count(tool),
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
            tool = negative_fixture_tool(stem)
            for kind in contexts:
                _rows, rejections, _counters, _tally = self.adapt_fixture(
                    stem,
                    tool=tool,
                    tool_base=self.base_of_kind(tool, kind),
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

