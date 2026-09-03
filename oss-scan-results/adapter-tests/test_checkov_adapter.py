"""The field-by-field test of the Checkov 3.3.12 native adapter.

``harness/lib/normalize/adapters/checkov.py`` is the only adapter whose input document
has **two mutually exclusive top-level shapes**, and it is the home of the user's own
worked path example. AAP 0.6.1 gives this file its row -- *"Asserts both output shapes
are handled, the leading-slash rule, the file_abs_path reconciliation, and that
passed_checks and skipped_checks produce no rows"* -- AAP 0.5.3 supplies the worked
example, AAP 0.5.4 fixes the behaviour, and AAP 0.9.4 puts this tree in the definition
of done. A failure here **halts the run** (AAP 0.9.2), which is why no assertion below
is softened into a smoke test.

No user-specified rule governs this file
----------------------------------------
No user-specified rule forces anything into this file's scope, and inventing one would be
fabrication; enterprise-standard best practice applies in its place, as AAP 0.7 and AAP
0.10.2 state, so the bar this file is held to is the AAP's own. That absence is expressly
**not** licence to lower it. Concretely, held to here: both top-level shapes are covered by
real committed fixtures rather than by one shape plus an argument; the positive fixture is
the runner's own artifact and is asserted to be, against
``harness/artifacts/raw/checkov.json`` rather than against a digest this tree records about
itself; the passes-and-skips exclusion is asserted from a fixture that actually contains
three passed checks and one skipped check, and it is named as derived because the artifact
carries neither bucket; every rejection class is asserted **by name** against a member of
``paths.REJECT_CLASSES``; and every row is compared field by field over the twelve fields
iterated from ``emit.FIELDS`` rather than spot-checked.

The three positive fixtures, and which claim each one makes
----------------------------------------------------------
``fixtures/checkov.json`` is the **capture**: the whole of
``harness/artifacts/raw/checkov.json``, byte for byte, so its sha256 equals the artifact's.
That is the strongest provenance available -- not a selection asserted to be faithful, but
the same bytes -- and :class:`RawArtifactProvenanceTests` measures it, along with the
envelope AAP 0.6.2 requires preserved: ``check_type``, the exact set of buckets present in
``results`` and the tool's own ``summary``.

``fixtures/checkov-alt-shape.json`` is **derived by shape transformation only**: the
capture's six failed checks as whole unedited objects, in order, inside the
multi-framework array form. :class:`SourceDocumentEqualityTests` compares the two
documents' ``failed_checks`` directly, as whole objects and in order, before any adapter
runs -- so an edit to a field the adapter never reads cannot hide behind a passing row
comparison.

``fixtures/derived-checkov-features.json`` is **derived** and says so. The runner invokes
checkov with ``--compact``, so the artifact's ``results`` object carries ``failed_checks``
alone: no ``passed_checks`` key, no ``skipped_checks`` key, no ``parsing_errors`` key. The
failures-only contract and the parsing-errors-as-status-evidence contract cannot be
asserted non-vacuously against a document with nothing to exclude, so those cases live in
this fixture -- five of the artifact's failed checks unedited, its sixth relocated into
``skipped_checks``, three passed checks and one parsing error. The capture keeps the other
half of the same contract: an absent bucket must read as zero.

The contract under test
-----------------------
``adapt(doc, *, tool, root, tool_base, allowlist, tally) -> (rows, rejections, counters)``,
with ``checkov.SCANNER_CLASS`` fixed at ``misconfig`` by AAP 0.5.4's class table -- it
never varies by ``check_type``, by framework or by record content. Field sources, from the
same table: ``rule_id`` <- ``check_id``; ``message`` <- ``check_name``; ``severity_native``
<- ``severity``, which requires a licence and is therefore ``null`` on every row in this
configuration; ``start_line`` <- ``file_line_range[0]``; ``path`` <- ``file_path`` with its
leading slash stripped, reconciled against ``file_abs_path``. The count unit is **one
element of** ``results.failed_checks[]`` -- the union across every report object, in either
shape.

Two shapes, and the shape changes with the content
--------------------------------------------------
The **object form** is a single report object carrying ``results.failed_checks[]``; the
**multi-framework form** is a top-level JSON array of ``{check_type, results: {...}}``
report objects. Which one appears is decided by the artifact's *content* -- one framework
reporting or several -- not by the invocation, so an implementation that handled only the
shape it happened to see first would work on one run and fail on the next. Both committed
fixtures are therefore exercised, and :class:`ShapeEquivalenceTests` asserts that the same
records in either form produce **identical rows in identical order**. That equality is the
assertion that proves shape handling is normalization rather than two divergent code paths;
it could not be asserted of two forks of the record loop.

The user's worked example, carried in unchanged (AAP 0.5.3)
-----------------------------------------------------------
Checkov's ``file_path`` is relative to the scan target **and carries a leading slash**, as
in ``/folder1/A.tf``, alongside ``file_abs_path`` holding the filesystem-absolute path and
``repo_file_path`` a root-relative path that also carries a leading slash. **Read that
slash as filesystem-absolute and the path relativizes to a long ``../`` chain, so the row
silently takes ``in_scope: false``.** Nothing crashes -- which is why
:class:`LeadingSlashTests` asserts the trap is real (the mis-reading genuinely produces a
``../`` chain, measured through ``paths.relativize_to_root``) and then asserts the emitted
path carries no ``../`` segment, is not absolute, and is in scope.

One recorded fact makes the reconciliation the *reliable* resolution route rather than a
cross-check: this provisioning's runner passes **one ``-d`` per expanded scope directory**,
eighteen of them in a single invocation, which is why the runner metadata records
``path_base.kind`` ``per_target_directory`` with ``anchor_fields``
``[repo_file_path, file_abs_path]``. A slash-stripped ``file_path`` is therefore relative
to *whichever target matched* -- so a strip-and-join against the tree root names a
directory that does not exist, **even once the slash is handled correctly**. An anchor
field is what disambiguates it.

Research basis
--------------
The path triple ``file_path`` / ``file_abs_path`` / ``repo_file_path`` is shown verbatim in
a real report at ``https://github.com/bridgecrewio/checkov/issues/3047``, and the JSON
output option that produces it is documented at
``https://www.checkov.io/2.Basics/CLI%20Command%20Reference.html``. AAP 0.2.2 records both,
together with the reason the adapter counts and emits **failed checks only**: the
``results`` object carries ``passed_checks``, ``skipped_checks`` and ``parsing_errors``
alongside the failures, and parsing errors are status evidence rather than findings.

What "irreconcilable" means here, stated so the reading is not left to inference
-------------------------------------------------------------------------------
AAP 0.5.3 requires a mismatch between the path fields to be **recorded rather than
silently resolved in favour of one field**, and AAP 0.1.3 requires a record that cannot be
attributed with certainty to be **rejected and counted, never guessed into a field**. Those
are two different situations and ``paths.resolve_checkov_path`` implements both:

* two anchors that *both* resolve but **disagree** -> the row is emitted from the first
  anchor in the recorded order, the disagreement is recorded on
  ``ResolvedPath.corroboration``, and ``path_corroboration_recorded`` counts it. This is
  the AAP 0.5.3 case, and it never suppresses a row;
* **no** anchor resolvable -- either no path field at all (``absent_path``), only a
  target-relative ``file_path`` under ``per_target_directory`` with no way to know which of
  eighteen targets it came from, or an anchor that cannot be expressed against the root at
  all (both ``unresolvable_path``) -> a counted rejection under a named class. This is the
  genuinely irreconcilable case, and it is what :class:`RejectionTests` asserts by name.

Both are asserted below. Where this file and the adapter disagree, the disagreement is the
finding: nothing under ``harness/lib/normalize/`` is edited from here, and a defect this
file reveals there is reported rather than repaired.

Rejection conditions this adapter can produce, each with a committed negative fixture
------------------------------------------------------------------------------------
``malformed_record``, ``missing_rule_id``, ``missing_message``, ``non_integer_start_line``,
``unresolvable_path`` and ``absent_path`` -- present whether or not this run's own artifact
contained the case, because a rejection path with no test is a rejection path nobody has
exercised.

The four this adapter **cannot** produce, with the reason each is out of reach:

* ``invalid_uri`` and any ``uriBaseId`` chain fault (base absent, cyclic, over-depth, no
  absolute ancestor) -- Checkov emits no SARIF ``uriBaseId`` and no base map, so there is no
  chain to walk. Those belong to ``adapters/sarif.py``;
* ``ambiguous_source_resolution`` -- that class describes two source files claiming one
  bytecode class key. This adapter's input is a configuration-file report and carries no
  bytecode, so no class-to-source resolution is ever attempted;
* ``unformable_package_coordinate`` -- AAP 0.5.4 attaches it to a *dependency-oriented*
  record. A misconfiguration names a location in a configuration file, not a package;
  ``package_coordinate`` is ``None`` by design on every row and its absence is permitted
  rather than a rejection;
* ``unattributable_section`` -- a Checkov report has one findings bucket,
  ``results.failed_checks[]``, and no per-record section to attribute. That class belongs
  to ``adapters/trivy.py``, whose ``scanner_class`` is the one that varies per record.

How to run it
-------------
From the repository root, and identically from any other working directory (every path is
derived from ``__file__``)::

    python3 -m unittest discover -s oss-scan-results/adapter-tests \\
        -p 'test_checkov_adapter.py' -v

Standard library only (AAP 0.4.1), so no plugin and no installed package is needed and
AAP 0.4.3 adds no dependency in any direction. There is no ``pytest`` import here and none
is required.

What this file deliberately does not do
---------------------------------------
It performs **no cross-tool interpretation of any kind** (AAP 0.3.2, restated in AAP
0.8.2): Checkov's misconfiguration rows are never compared with, ranked against or
explained by another tool's output, in code or in a comment, and no other tool's fixture is
loaded here. It compares nothing against Apex, Cantina or any other scanner. It judges no
finding real, important, a false positive or a duplicate, and it deduplicates nothing --
two identical failed checks are two records and two rows. It references no exit code:
Checkov exits non-zero *precisely because* it found something, artifact status and exit
status are independent (AAP 0.5.4), and the exit code belongs to ``<tool>.status``.

No secret value appears in any assertion, literal, message or docstring here, and no
``check_result.results_configuration`` content is read into anything -- this tree is
committed to git, since ``.gitignore:31`` ignores only ``artifacts/``. The fixtures are
read and never written: their sha256 digests are asserted against the digests the expected
files record, so an edit to one becomes visible rather than silently changing what these
tests mean. ``harness/artifacts/raw/checkov.json`` is likewise opened read-only and only
for the provenance comparison -- that tree is runner-only (AAP 0.8.1), and nothing here
writes to it, creates it or clears it. Where a shape this tree has no committed fixture for is needed -- a report
object wrapped into the array form for a negative condition, a licensed ``severity``
literal, a hostile path -- the document is built **in memory** from named minimal records;
no fixture file is transformed, and the alt-shape fixture used for the equivalence
assertion is the committed derived file rather than an in-test transformation of the
captured one.
"""

from __future__ import annotations

# Standard library only (AAP 0.4.1), and only these six:
#   json     -- parse a fixture or an expected file without mutating either;
#   hashlib  -- confirm each fixture is byte-for-byte the file its expected result was
#               derived from (AAP 0.4.1 names hashlib among this run's stdlib set);
#   sys      -- the one-time sys.path bootstrap below;
#   tempfile -- the hermetic scan roots, allowlists and runner metadata;
#   unittest -- the runner, so the suite needs no third-party plugin;
#   pathlib  -- every location derived from __file__ rather than from the working
#               directory the runner happened to start in.
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

# --------------------------------------------------------------------------------------
# The one-time sys.path bootstrap.
#
# There is deliberately no __init__.py under harness/lib/normalize/ or its adapters/
# directory: PEP 420 implicit namespace packages make "from normalize import paths" work
# once harness/lib is on sys.path. cli.py owns the same two lines for its own
# direct-script route (AAP 0.6.4), and this file mirrors them rather than assuming an
# installed package, because nothing installs this tree.
#
# This file sits at <repo>/oss-scan-results/adapter-tests/, so parents[2] is the
# repository root. The entry is derived from __file__ and not from the working directory,
# so the suite imports identically whether it is discovered from the repository root or
# from anywhere else. The membership guard makes repeated insertion idempotent: unittest
# discovery imports sibling test modules that perform the same insertion, and a duplicate
# path entry is noise that outlives the run in sys.path.
# --------------------------------------------------------------------------------------
_THIS_FILE = Path(__file__).resolve()
_TESTS_DIR = _THIS_FILE.parent
REPO_ROOT = _THIS_FILE.parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

# The three modules AAP 0.6.4 permits an adapter to depend on, plus the adapter itself.
# reconcile.py is deliberately **not** imported: it is not among this file's declared
# dependencies, and the count unit is asserted instead by an independent traversal written
# here (:func:`failed_check_union`) and cross-checked against each expected file's
# hand-verified ``counts.raw_finding_records``. That is stronger than calling the same
# function the adapter is being compared against.
from normalize import emit, paths, severity  # noqa: E402  (import follows the bootstrap)
from normalize.adapters import checkov  # noqa: E402  (import follows the bootstrap)

#: This tree. Both directories are inputs and are never written to by this module.
FIXTURES_DIR = _TESTS_DIR / "fixtures"
EXPECTED_DIR = _TESTS_DIR / "expected"

# --------------------------------------------------------------------------------------
# The twelve authoritative scope globs (AAP 0.3.1), byte-exact and in the request's order.
#
# Restated here rather than read from paths.ALLOWLIST_GLOBS on purpose: this module writes
# these twelve lines to its own allowlist file, loads them back through
# paths.load_allowlist() and only then confirms the loaded tuple is what paths.py authors,
# through paths.allowlist_matches_authoritative_globs(). Loading the module's own copy and
# comparing it with itself would assert nothing.
#
# There is no exclusion line: the literal `src/test` exclusion lives in paths.py, not in
# the allowlist (AAP 0.3.1), and adding one here would change what every in_scope
# assertion below means.
# --------------------------------------------------------------------------------------
AUTHORITATIVE_GLOBS = (
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

# --------------------------------------------------------------------------------------
# What the runner metadata records for this tool -- read as input, never inferred from an
# artifact (AAP 0.5.4: "every base taken from the recorded runner metadata").
#
# The source is harness/artifacts/logs/runner-metadata.json, whose checkov entry records
# path_base.kind `per_target_directory`, value `/opt/spark-src`, anchor_fields
# `[repo_file_path, file_abs_path]`, and an invocation_form of 18 `-d` target roots in one
# invocation. :class:`RecordedMetadataAgreementTests` asserts these four constants against
# that document rather than trusting this restatement of it.
#
# The recorded scan root is used as a *string*: neither this adapter nor paths.py reads the
# filesystem to resolve a path, which :class:`RootDependenceTests` establishes by producing
# byte-identical rows under the recorded root, under a materialised temporary tree and under
# a root that exists on no filesystem at all. That independence is also why a row's `path`
# may legitimately name something that is not a file on disk -- the count AAP 0.6.1 has
# run-record.md report.
# --------------------------------------------------------------------------------------
RECORDED_METADATA_PATH = REPO_ROOT / "harness" / "artifacts" / "logs" / "runner-metadata.json"
RECORDED_SCAN_ROOT = "/opt/spark-src"
RECORDED_PATH_BASE_KIND = "per_target_directory"
RECORDED_ANCHOR_FIELDS = ("repo_file_path", "file_abs_path")
RECORDED_INVOCATIONS_PER_RUN = 1

# --------------------------------------------------------------------------------------
# Real paths, verified present in the pinned tree at 59b8a448 rather than invented.
#
# The realistic in-scope Checkov surface is the Kubernetes docker tree: it is scanned by
# the file-based tools while being no Maven module at all, which is a standing reminder
# that scanned scope and build scope are different things -- and the reason a real
# Dockerfile is used here instead of an invented `.tf` path.
# --------------------------------------------------------------------------------------
SPARK_DOCKERFILE = "resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile"
PYTHON_DOCKERFILE = (
    "resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile"
)
R_DOCKERFILE = (
    "resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/R/Dockerfile"
)

#: A file reached only through the *mid-path* `**` of the volcano source root, which
#: -Pvolcano adds at resource-managers/kubernetes/core/pom.xml:66. A second witness for the
#: zero-or-more-directories semantics, on a different number of absorbed segments.
VOLCANO_FEATURE_STEP = (
    "resource-managers/kubernetes/core/volcano/src/main/scala/org/apache/spark/deploy/k8s"
    "/features/VolcanoFeatureStep.scala"
)

#: An in-scope Python test module. It carries **no** `src/test` segment and sits inside the
#: authoritative glob `python/pyspark/**`, so the loose reading of "tests are out of scope"
#: must not flip it (AAP 0.3.1: 832 such modules are scanned).
PYSPARK_TEST_MODULE = "python/pyspark/ml/tests/test_evaluation.py"

#: A real Scala test path. The exclusion is the literal `src/test`, and it overrides a
#: positive glob match -- this path is under `core/src/` and would otherwise match
#: `core/src/main/**` on nothing, so the assertion is that it is out of scope for the
#: stated reason rather than by accident.
SCALA_TEST_SOURCE = "core/src/test/scala/org/apache/spark/SparkFunSuite.scala"

#: Real files inside the root but outside all twelve globs -- the pin's three lockfiles.
#: A dependency-oriented runner legitimately reaches them; the row is kept with
#: `in_scope: false` and counted, never dropped (AAP 0.9.3).
OUT_OF_SCOPE_FILES = (
    "dev/package-lock.json",
    "ui-test/package-lock.json",
    "docs/Gemfile.lock",
)

#: The glob every Dockerfile row must match, and the one whose mid-path `**` a naive
#: matcher drops. AAP 0.5.4 requires the matcher to implement true zero-or-more-directories
#: semantics, which Python's fnmatch and PurePath.match do not provide.
KUBERNETES_GLOB = "resource-managers/kubernetes/**/src/main/**"

#: Only the paths these tests actually reference are materialised under the temporary tree
#: root (see :class:`Environment`), so the tree contains nothing this file does not name.
MATERIALISED_TREE_FILES = (
    SPARK_DOCKERFILE,
    PYTHON_DOCKERFILE,
    R_DOCKERFILE,
    VOLCANO_FEATURE_STEP,
    PYSPARK_TEST_MODULE,
    SCALA_TEST_SOURCE,
    *OUT_OF_SCOPE_FILES,
)

# --------------------------------------------------------------------------------------
# The committed fixtures. Every one is read; none is written.
# --------------------------------------------------------------------------------------

#: The capture: the whole raw artifact this run's checkov runner wrote, in the object form,
#: byte for byte. Not an excerpt of it and not a selection from it -- the artifact is 8,380
#: bytes, so no excerpting was needed. :class:`RawArtifactProvenanceTests` asserts that
#: against ``harness/artifacts/raw/checkov.json`` rather than against a digest this tree
#: owns, which is the only comparison that establishes provenance rather than
#: self-consistency.
CAPTURED_FIXTURE = "checkov"

#: The capture's records in the multi-framework array form. A **derived** file created by
#: shape transformation only, with no field value edited -- which is why the equivalence
#: assertion uses it rather than an in-test transformation of the captured fixture. That
#: the transformation really is shape-only is asserted directly by
#: :class:`SourceDocumentEqualityTests`, on the two committed documents and before any
#: adapter runs.
ALT_SHAPE_FIXTURE = "checkov-alt-shape"

#: The derived feature fixture. The captured artifact was written under ``--compact`` and
#: its ``results`` object carries ``failed_checks`` alone, so the failures-only contract and
#: the parsing-errors-as-status-evidence contract cannot be asserted non-vacuously against
#: it: there is nothing to exclude. Those cases are not deleted -- they live here, in a file
#: whose expected result declares it derived, names what it was derived from and names the
#: cases it exists for. Five of the artifact's six failed checks are carried across
#: unedited; the sixth is relocated into ``skipped_checks``; three passed checks and one
#: parsing error follow Checkov 3.3.12's own documented report shape.
FEATURE_FIXTURE = "derived-checkov-features"

#: The two documents whose ``failed_checks`` must be the same records in the same order.
#: Named as a pair because several assertions are about the pair rather than about either
#: file, and because a third positive fixture joining ``POSITIVE_FIXTURES`` must not be
#: silently drawn into a shape-equivalence claim that is not made of it.
SHAPE_PAIR = (CAPTURED_FIXTURE, ALT_SHAPE_FIXTURE)

#: Every fixture that is declared derived rather than captured. Both are held to the
#: declaration: each must say so in its expected file and each must be demonstrably not the
#: raw artifact (:meth:`RawArtifactProvenanceTests.test_each_derived_fixture_is_declared_derived_and_is_not_the_artifact`).
DERIVED_FIXTURES = (ALT_SHAPE_FIXTURE, FEATURE_FIXTURE)

#: Every fixture that maps to rows rather than to rejections. All three are compared field
#: for field against their own hand-verified expectations.
POSITIVE_FIXTURES = (CAPTURED_FIXTURE, *DERIVED_FIXTURES)

#: One negative fixture per rejection condition this adapter can produce (AAP 0.5.4,
#: AAP 0.6.2), each present whether or not this run's artifact contained the case, followed
#: by the committed fixtures that reach an already-covered class by a DIFFERENT route. The
#: unresolvable-path fixture carries both sub-reasons of its class, and the
#: non-integer-start-line fixture carries the malformed-container shapes beside the
#: element-type one, so the class boundary between them is exercised rather than assumed.
#:
#: The route variants are not redundancy. ``file_line_range`` can be defective as a wrong
#: container (absent, empty, a number, a string, an object) or as a wrong first element
#: (``true``, ``0``, a negative integer), and those two defects take DIFFERENT classes --
#: ``malformed_record`` for the container and ``non_integer_start_line`` for the element --
#: so a corpus carrying one of each cannot show where the boundary lies.
#: :class:`RejectionTests` drives every fixture here through the same field-by-field row,
#: counter and reconciliation comparison, and the class each yields is read from its own
#: expectation rather than from its slug.
#:
#: Eight of these fixtures carry a defective ``file_line_range``, and their relationship is
#: what makes each one attributable. Seven -- the boolean, zero, negative, empty, number,
#: string and object forms -- are variants of one three-record document differing at exactly
#: ``/results/failed_checks/1/file_line_range``, so the rejection is the only difference
#: between any two of them: same two rows, same three bucket counts, same 28 counters. The
#: eighth, ``reject-checkov-non-integer-start-line``, carries the element defect and three
#: malformed containers side by side in a single document, which is what holds the class
#: boundary inside one artifact rather than across two.
REJECT_FIXTURES = (
    "reject-checkov-unresolvable-path",
    "reject-checkov-missing-rule-id",
    "reject-checkov-missing-message",
    "reject-checkov-non-integer-start-line",
    "reject-checkov-malformed-record",
    "reject-checkov-absent-path",
    "reject-checkov-boolean-start-line",
    "reject-checkov-empty-line-range",
    "reject-checkov-empty-message",
    "reject-checkov-line-range-number",
    "reject-checkov-line-range-object",
    "reject-checkov-line-range-string",
    "reject-checkov-negative-start-line",
    "reject-checkov-unresolvable-path-uri-anchor",
    "reject-checkov-zero-start-line",
)

ALL_FIXTURES = (*POSITIVE_FIXTURES, *REJECT_FIXTURES)

#: The runner's own artifact, opened read-only by :class:`RawArtifactProvenanceTests`.
#:
#: ``harness/artifacts/raw/`` is runner-only (AAP 0.8.1): nothing in this module writes to
#: it, creates it or clears it. It is read for one purpose -- so that the captured fixture's
#: provenance is a measurement against the tool's own output rather than a claim checkable
#: only against a digest this tree records about itself.
RAW_ARTIFACT_PATH = REPO_ROOT / "harness" / "artifacts" / "raw" / "checkov.json"

#: The six rejection classes this adapter can produce, spelled through ``paths.py``'s own
#: constants so a typo cannot invent a class. The other four members of the closed ten are
#: unreachable from here for the reasons this module's docstring gives -- no SARIF bases, no
#: bytecode input, no package coordinate to form, and one findings bucket rather than
#: sections. :class:`ProducibleClassBoundaryTests` asserts that partition explicitly.
PRODUCIBLE_REJECT_CLASSES = (
    paths.REJECT_ABSENT_PATH,
    paths.REJECT_UNRESOLVABLE_PATH,
    paths.REJECT_MISSING_RULE_ID,
    paths.REJECT_MISSING_MESSAGE,
    paths.REJECT_NON_INTEGER_START_LINE,
    paths.REJECT_MALFORMED_RECORD,
)

#: The four this adapter cannot reach, each with the reason it is out of reach. Kept as
#: data rather than prose so the partition is asserted rather than asserted-about.
UNREACHABLE_REJECT_CLASSES = {
    paths.REJECT_INVALID_URI: (
        "checkov emits no SARIF uri, no uriBaseId and no originalUriBaseIds base map, so "
        "there is no URI reference to parse and no chain to walk"
    ),
    paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION: (
        "the input is a configuration-file report carrying no bytecode class, so no "
        "class-to-source resolution is attempted and none can be ambiguous"
    ),
    paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE: (
        "AAP 0.5.4 attaches this class to a dependency-oriented record; a misconfiguration "
        "names a location in a configuration file, so package_coordinate is absent by "
        "design and its absence is permitted rather than a rejection"
    ),
    paths.REJECT_UNATTRIBUTABLE_SECTION: (
        "a checkov report has one findings bucket, results.failed_checks[], and no "
        "per-record section to attribute; scanner_class is fixed at misconfig"
    ),
}

#: The one bucket that holds findings, and the two that never do, spelled as literals.
#:
#: Literals rather than ``checkov.EMITTED_RESULT_SECTION`` and
#: ``checkov.NEVER_EMITTED_RESULT_SECTIONS`` on purpose: the independent traversal below
#: must not take its bucket name from the module it is checking, or a rename there would
#: move both sides together and the count would agree for the wrong reason.
#: :meth:`AdapterContractTests.test_the_emitted_and_never_emitted_sections_are_the_named_buckets`
#: asserts the adapter's own constants equal these literals, so the two are tied together
#: by an assertion rather than by a shared reference.
EMITTED_SECTION = "failed_checks"
NEVER_EMITTED_SECTIONS = ("passed_checks", "skipped_checks")
PARSING_ERRORS_SECTION = "parsing_errors"


# --------------------------------------------------------------------------------------
# Fixture and expectation loading
# --------------------------------------------------------------------------------------


def fixture_path(stem: str) -> Path:
    """Return the committed fixture's location. Read-only, always."""
    return FIXTURES_DIR / f"{stem}.json"


def expected_path(stem: str) -> Path:
    """Return the hand-verified expected file's location for one fixture."""
    return EXPECTED_DIR / f"{stem}.rows.json"


def load_fixture(stem: str):
    """Parse one committed fixture, returning a fresh document on every call.

    Fresh matters: a test that wraps a report object into the array form, or that builds a
    variant from it, must not be able to affect another test through a shared object. The
    file itself is never written -- :class:`FixtureInventoryTests` pins each fixture's
    sha256 against the digest its expected file records.
    """
    return json.loads(fixture_path(stem).read_text(encoding="utf-8"))


def load_expected(stem: str) -> dict:
    """Parse one hand-verified expected file.

    Each carries the rows, the counts, all of the adapter's counters, the per-row
    derivations and the per-rejection class, detail and record identity. Its own
    ``description`` states the authority: where it and the adapter disagree, the
    disagreement is the finding rather than a diff to apply.
    """
    return json.loads(expected_path(stem).read_text(encoding="utf-8"))


def sha256_of(path: Path) -> str:
    """Return the hex sha256 of a file, read as bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


# --------------------------------------------------------------------------------------
# The independent count-unit traversal.
#
# AAP 0.5.4 fixes the count unit as one element of results.failed_checks[], the **union**
# across every report object in either shape. This walks that unit and builds nothing: no
# row, no path resolution, no severity mapping. It is written here rather than imported so
# that the number the identity is checked against does not come from the module under test
# -- a count taken from the traversal that builds the rows would satisfy the assertion
# while testing nothing. Every result is additionally cross-checked against the
# hand-verified ``counts.raw_finding_records`` in the expected file.
# --------------------------------------------------------------------------------------


def failed_check_union(doc) -> list:
    """Return every ``results.failed_checks[]`` element across the document, in order.

    Both top-level shapes reduce the same way a reader would reduce them: a list is the
    sequence of report objects, and a mapping is a one-element sequence. An element that is
    not an object, a ``results`` that is not an object and a ``failed_checks`` that is not
    an array each contribute nothing -- deliberately counted as zero rather than treated as
    an error, because that is what makes ``rows + rejections`` comparable with this number
    on a malformed artifact.

    ``passed_checks``, ``skipped_checks`` and ``parsing_errors`` are not read at all, which
    is the failures-only contract expressed as a traversal rather than as a claim.
    """
    reports = doc if isinstance(doc, list) else [doc]
    union: list = []
    for report in reports:
        if not isinstance(report, dict):
            continue
        results = report.get("results")
        if not isinstance(results, dict):
            continue
        failed = results.get(EMITTED_SECTION)
        if not isinstance(failed, list):
            continue
        union.extend(failed)
    return union


def bucket_counts(doc) -> dict:
    """Return the four ``results`` bucket sizes across the document, summed in order.

    What a fixture contains is established from the document itself before anything is
    asserted about what the adapter did with it, which is what keeps the failures-only
    contract non-degenerate: an assertion that passed over a fixture carrying no passed
    checks and no skipped checks would establish nothing at all.
    """
    reports = doc if isinstance(doc, list) else [doc]
    totals = {
        "failed_checks": 0,
        "passed_checks": 0,
        "skipped_checks": 0,
        "parsing_errors": 0,
    }
    for report in reports:
        if not isinstance(report, dict):
            continue
        results = report.get("results")
        if not isinstance(results, dict):
            continue
        for bucket in totals:
            value = results.get(bucket)
            if isinstance(value, list):
                totals[bucket] += len(value)
    return totals


def bucket_records(doc, bucket: str) -> list:
    """Return every element of one ``results`` bucket across the document, in order."""
    reports = doc if isinstance(doc, list) else [doc]
    found: list = []
    for report in reports:
        if not isinstance(report, dict):
            continue
        results = report.get("results")
        if not isinstance(results, dict):
            continue
        value = results.get(bucket)
        if isinstance(value, list):
            found.extend(value)
    return found


# --------------------------------------------------------------------------------------
# The hermetic environments.
#
# Every file any of them needs is created inside one tempfile.TemporaryDirectory, and both
# configuration files are read back through paths.py's own loaders rather than handed to
# the adapter as literals -- so the loaders are exercised on the same route cli.py uses.
#
# Three environments, because three different recorded configurations have to be exercised
# and each is a property of the metadata rather than of a record:
#
#   RECORDED       the provisioning as recorded: per_target_directory, both anchors, rooted
#                  at the recorded scan root. This is the environment the expected files'
#                  counters were derived under, so it is the one they are compared against.
#   TREE           the same base kind, rooted at a materialised temporary tree. Rows must
#                  be byte-identical to the recorded environment's; the one counter that
#                  legitimately differs is path_corroboration_recorded, because an absolute
#                  file_abs_path naming another root then relativizes to a ../ chain and
#                  disagrees with repo_file_path. checkov.rows.json's counters_root_note
#                  states exactly that, and :class:`RootDependenceTests` asserts it.
#   SINGLE_TARGET  path_base.kind scan_root -- one target equal to the scan root. The only
#                  configuration in which a record carrying nothing but file_path resolves
#                  rather than being rejected, so it is the only way to exercise that
#                  documented fallback branch honestly.
# --------------------------------------------------------------------------------------


class Environment:
    """One hermetic scan root with its own allowlist file and runner metadata.

    Attributes:
        name: Which of the three configurations this is, for assertion messages.
        directory: The subdirectory of the module temporary directory holding its files.
        root: The absolute scan root the adapter expresses paths against.
        globs: The twelve globs, as ``paths.load_allowlist`` returned them from the file
            this object wrote.
        metadata: The runner-metadata document, as ``paths.load_runner_metadata`` returned
            it from the file this object wrote.
        tool_base: The ``paths.ToolPathBase`` for ``checkov``, taken from that document
            through ``paths.tool_path_base``.
    """

    def __init__(
        self,
        directory: Path,
        *,
        name: str,
        root: str,
        base_kind: str,
        anchor_fields: tuple[str, ...],
        materialise: bool = False,
    ) -> None:
        """Write and load this environment's allowlist and runner metadata."""
        self.name = name
        self.directory = directory
        self.root = root
        directory.mkdir(parents=True, exist_ok=True)

        # One glob per line, byte-exact, with a trailing newline and nothing else. No
        # exclusion line: the src/test exclusion is paths.py's, not the allowlist's.
        self.allowlist_path = directory / "allowlist.txt"
        self.allowlist_path.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS), encoding="utf-8"
        )
        self.globs = paths.load_allowlist(self.allowlist_path)

        self.metadata_path = directory / "runner-metadata.json"
        self.metadata_path.write_text(
            json.dumps(self._metadata_document(base_kind, anchor_fields), indent=1) + "\n",
            encoding="utf-8",
        )
        self.metadata = paths.load_runner_metadata(self.metadata_path)
        self.tool_base = paths.tool_path_base(self.metadata, checkov.TOOL)

        if materialise:
            # Only the paths these tests reference, and each as an empty file: the point is
            # that the tree exists and that resolution does not consult it. Nothing here
            # writes into the pinned tree or into the working checkout.
            for relative in MATERIALISED_TREE_FILES:
                target = Path(root) / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.touch()

    def _metadata_document(self, base_kind: str, anchor_fields: tuple[str, ...]) -> dict:
        """Build the minimal document ``paths.load_runner_metadata`` accepts.

        Minimal is deliberate. It carries the base facts a resolver needs -- the recorded
        kind, the base value, the anchor field order and the invocation form that makes
        ``file_path`` ambiguous on its own -- and nothing that would turn this test into a
        second copy of the run's record. The direction AAP 0.6.4 fixes is preserved: the
        metadata is *input* to the normalizer, and ``tool-status.md`` is rendered from it
        afterwards, so nothing here reads a rendered document.
        """
        path_base: dict = {"kind": base_kind, "value": self.root}
        if anchor_fields:
            path_base["anchor_fields"] = list(anchor_fields)
        return {
            "purpose": (
                "Minimal runner metadata for the checkov adapter test. Written and read "
                "inside a temporary directory; it is not the run's record."
            ),
            "spark_src": self.root,
            "tools": {
                checkov.TOOL: {
                    "canonical_tool_identifier": checkov.TOOL,
                    "scanner_class": checkov.SCANNER_CLASS,
                    "path_base": path_base,
                    "invocation_form": {
                        "invocations_per_run": RECORDED_INVOCATIONS_PER_RUN,
                        "target_passing_style": (
                            "18 root-relative -d target roots in one invocation"
                        ),
                    },
                    "working_directory": {"path": self.root, "equals_scan_root": True},
                    "resolved_scan_root": self.root,
                }
            },
        }


#: Module-level state, built once in :func:`setUpModule` and released in
#: :func:`tearDownModule`. Held at module level because every test needs the same roots,
#: and rebuilding them per test would make each test's rows depend on a different
#: temporary path.
_TEMPORARY_DIRECTORY: tempfile.TemporaryDirectory | None = None
RECORDED_ENV: Environment | None = None
TREE_ENV: Environment | None = None
SINGLE_TARGET_ENV: Environment | None = None


def setUpModule() -> None:
    """Create the three hermetic environments inside one temporary directory."""
    global _TEMPORARY_DIRECTORY, RECORDED_ENV, TREE_ENV, SINGLE_TARGET_ENV
    _TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory(prefix="blitzy-checkov-adapter-")
    base = Path(_TEMPORARY_DIRECTORY.name)
    RECORDED_ENV = Environment(
        base / "recorded",
        name="recorded",
        root=RECORDED_SCAN_ROOT,
        base_kind=RECORDED_PATH_BASE_KIND,
        anchor_fields=RECORDED_ANCHOR_FIELDS,
    )
    TREE_ENV = Environment(
        base / "tree",
        name="tree",
        root=str(base / "tree" / "spark-src"),
        base_kind=RECORDED_PATH_BASE_KIND,
        anchor_fields=RECORDED_ANCHOR_FIELDS,
        materialise=True,
    )
    SINGLE_TARGET_ENV = Environment(
        base / "single-target",
        name="single_target",
        root=RECORDED_SCAN_ROOT,
        base_kind=paths.PATH_BASE_KIND_SCAN_ROOT,
        anchor_fields=(),
    )


def tearDownModule() -> None:
    """Release the temporary directory. Nothing this module wrote survives it."""
    global _TEMPORARY_DIRECTORY, RECORDED_ENV, TREE_ENV, SINGLE_TARGET_ENV
    RECORDED_ENV = None
    TREE_ENV = None
    SINGLE_TARGET_ENV = None
    if _TEMPORARY_DIRECTORY is not None:
        _TEMPORARY_DIRECTORY.cleanup()
        _TEMPORARY_DIRECTORY = None


def recorded_env() -> Environment:
    """Return the recorded-configuration environment, or fail loudly if absent."""
    if RECORDED_ENV is None:  # pragma: no cover - defended, unreachable under unittest
        raise RuntimeError("setUpModule did not run: the recorded environment is missing")
    return RECORDED_ENV


def tree_env() -> Environment:
    """Return the materialised-tree environment, or fail loudly if absent."""
    if TREE_ENV is None:  # pragma: no cover - defended, unreachable under unittest
        raise RuntimeError("setUpModule did not run: the tree environment is missing")
    return TREE_ENV


def single_target_env() -> Environment:
    """Return the single-target (scan_root) environment, or fail loudly if absent."""
    if SINGLE_TARGET_ENV is None:  # pragma: no cover - defended, unreachable
        raise RuntimeError("setUpModule did not run: the single-target environment is missing")
    return SINGLE_TARGET_ENV


# --------------------------------------------------------------------------------------
# Synthetic documents.
#
# Used only where this tree carries no committed fixture for the shape a requirement names
# -- a licensed severity literal, a hostile path, a disagreeing anchor pair, failed checks
# spread across more than one report object. Each record is built from named fields and
# nothing else, so a call site states exactly what the adapter will see; nothing is
# defaulted in behind the caller's back, and no field appears that the test did not ask
# for. No `check_result` and no `results_configuration` content is ever built: those carry
# the scanned file's own command text, and this tree is committed to git.
# --------------------------------------------------------------------------------------

#: Sentinel for "do not put this key in the record at all", which is a different input from
#: a key present with a JSON null -- a distinction several rejection conditions turn on.
_OMIT = object()


def failed_check(
    *,
    check_id=_OMIT,
    check_name=_OMIT,
    file_path=_OMIT,
    file_abs_path=_OMIT,
    repo_file_path=_OMIT,
    file_line_range=_OMIT,
    severity_literal=_OMIT,
    caller_file_path=_OMIT,
    guideline=_OMIT,
) -> dict:
    """Build one ``results.failed_checks[]`` element carrying only the fields named.

    ``severity_literal`` is spelled out rather than named ``severity`` so the keyword cannot
    be confused with the :mod:`normalize.severity` module at a call site; the key written
    into the record is ``severity``, which is the field AAP 0.5.4 names.
    """
    record: dict = {}
    for key, value in (
        ("check_id", check_id),
        ("check_name", check_name),
        ("file_path", file_path),
        ("file_abs_path", file_abs_path),
        ("repo_file_path", repo_file_path),
        ("file_line_range", file_line_range),
        ("severity", severity_literal),
        ("caller_file_path", caller_file_path),
        ("guideline", guideline),
    ):
        if value is not _OMIT:
            record[key] = value
    return record


#: The check identifier and name every synthetic record carries unless a test overrides
#: them. Both are real values from the captured artifact, so no invented rule identifier or
#: invented prose enters an assertion.
SYNTHETIC_CHECK_ID = "CKV_DOCKER_2"
SYNTHETIC_CHECK_NAME = "Ensure that HEALTHCHECK instructions have been added to container images"


def anchored_check(relative_path: str, root: str, **overrides) -> dict:
    """Build a well-formed failed check for one root-relative path.

    The three path fields are written exactly as Checkov writes them: ``repo_file_path``
    root-relative **with** a leading slash, ``file_abs_path`` filesystem-absolute under the
    given root, and ``file_path`` target-relative with a leading slash -- here the last two
    segments, which is what a record from one of the runner's eighteen ``-d`` roots looks
    like.

    ``overrides`` are member names as the artifact spells them. A value of :data:`_OMIT`
    removes that member, so a hostile variant differs from a sound one by exactly the field
    under test and nothing else.
    """
    segments = relative_path.split("/")
    target_relative = "/" + "/".join(segments[-2:] if len(segments) > 1 else segments)
    record = {
        "check_id": SYNTHETIC_CHECK_ID,
        "check_name": SYNTHETIC_CHECK_NAME,
        "file_path": target_relative,
        "repo_file_path": f"/{relative_path}",
        "file_abs_path": f"{root}/{relative_path}",
        "file_line_range": [1, 1],
    }
    for member, value in overrides.items():
        if value is _OMIT:
            record.pop(member, None)
        else:
            record[member] = value
    return record


def report(
    *failed,
    check_type: str = "dockerfile",
    passed=(),
    skipped=(),
    parsing_errors=(),
    results=_OMIT,
) -> dict:
    """Build one report object: a ``check_type`` and a ``results`` container.

    ``results`` overrides the whole container, which is how the container shapes that
    contribute no record -- a ``results`` that is not an object, a ``failed_checks`` that is
    not an array -- are built without any other field changing.
    """
    if results is not _OMIT:
        return {"check_type": check_type, "results": results}
    return {
        "check_type": check_type,
        "results": {
            EMITTED_SECTION: list(failed),
            NEVER_EMITTED_SECTIONS[0]: list(passed),
            NEVER_EMITTED_SECTIONS[1]: list(skipped),
            PARSING_ERRORS_SECTION: list(parsing_errors),
        },
    }


def object_form(*failed, **kwargs) -> dict:
    """Build the object form: one report object as the whole document."""
    return report(*failed, **kwargs)


def array_form(*reports) -> list:
    """Build the multi-framework form: a top-level array of report objects."""
    return list(reports)


# --------------------------------------------------------------------------------------
# The shared base: one way to call the adapter, and one way to assert about a result.
# --------------------------------------------------------------------------------------


class Adapted:
    """One adaptation and the independent record count taken beside it.

    Both sides of ``raw finding records = dataset rows + rejected records`` are held
    together so a test asserts over one pair of measurements rather than taking a second
    one. They remain two code paths: ``checkov.adapt`` builds rows, and
    :func:`failed_check_union` walks count units and builds nothing.
    """

    __slots__ = ("document", "rows", "rejections", "counters", "tally", "raw_records", "env")

    def __init__(self, env, document, rows, rejections, counters, tally, raw_records) -> None:
        """Hold one adaptation's inputs, outputs and independent count."""
        self.env = env
        self.document = document
        self.rows = rows
        self.rejections = rejections
        self.counters = counters
        self.tally = tally
        self.raw_records = raw_records

    @property
    def classes(self) -> list:
        """Every rejection's class, in emission order."""
        return [rejection.reject_class for rejection in self.rejections]

    @property
    def by_class(self) -> dict:
        """Rejection counts per named class, tallied as ``cli.py`` tallies them."""
        counted: dict = {}
        for rejection in self.rejections:
            counted[rejection.reject_class] = counted.get(rejection.reject_class, 0) + 1
        return counted

    @property
    def paths_emitted(self) -> list:
        """Every emitted row's ``path``, in row order."""
        return [row["path"] for row in self.rows]


class CheckovAdapterTestCase(unittest.TestCase):
    """Shared plumbing: one call route into the adapter and the assertions used throughout.

    Nothing here weakens an assertion. Each helper exists so that the same check is spelled
    the same way everywhere -- a field-by-field row comparison that names the field and the
    row index when it fails is worth more than twelve inline ``assertEqual`` calls, and a
    rejection helper that asserts the class **by name** is what stops a test from counting
    rejections without telling one condition from another.
    """

    maxDiff = None

    # ---------------------------------------------------------------- calling the adapter

    def adapt(self, document, *, env=None, **overrides) -> Adapted:
        """Adapt one document through the uniform entry point and count it independently.

        ``overrides`` replaces a keyword argument, which is how the caller-fault tests hand
        the adapter something its contract forbids without a second call site.
        """
        environment = env if env is not None else recorded_env()
        tally = severity.LiteralTally.with_all_tools()
        keywords = {
            "tool": checkov.TOOL,
            "root": environment.root,
            "tool_base": environment.tool_base,
            "allowlist": environment.globs,
            "tally": tally,
        }
        keywords.update(overrides)
        rows, rejections, counters = checkov.adapt(document, **keywords)
        return Adapted(
            env=environment,
            document=document,
            rows=rows,
            rejections=rejections,
            counters=counters,
            tally=keywords["tally"],
            raw_records=len(failed_check_union(document)),
        )

    def adapt_fixture(self, stem: str, *, env=None) -> Adapted:
        """Adapt one committed fixture, loaded fresh."""
        return self.adapt(load_fixture(stem), env=env)

    # ------------------------------------------------------------------ row-level asserts

    def assertRowFields(self, row, expected_row, *, row_index: int, context: str) -> None:
        """Assert one row against its expectation over all twelve fields, in order.

        The field list is iterated from ``emit.FIELDS`` -- the authored constant everything
        downstream keys on -- rather than restated here, so a row carrying an extra key or
        missing one is caught by the key-set check and every value is compared under the
        name the dataset uses.
        """
        self.assertEqual(
            set(row),
            set(emit.FIELDS),
            msg=f"{context}: row {row_index} must carry exactly the twelve fields",
        )
        self.assertEqual(
            list(row),
            list(emit.FIELDS),
            msg=f"{context}: row {row_index} must carry the fields in emit.FIELDS order",
        )
        for field in emit.FIELDS:
            self.assertEqual(
                row[field],
                expected_row[field],
                msg=(
                    f"{context}: row {row_index} field {field!r} -- expected "
                    f"{expected_row[field]!r}, observed {row[field]!r}"
                ),
            )

    def assertRowsMatchExpected(self, adapted: Adapted, expected: dict, *, context: str) -> None:
        """Assert every emitted row against the expected file, field by field and in order."""
        expected_rows = expected["rows"]
        self.assertEqual(
            len(adapted.rows),
            len(expected_rows),
            msg=(
                f"{context}: expected {len(expected_rows)} rows, observed "
                f"{len(adapted.rows)} -- {[row.get('rule_id') for row in adapted.rows]}"
            ),
        )
        for index, (row, expected_row) in enumerate(zip(adapted.rows, expected_rows)):
            self.assertRowFields(row, expected_row, row_index=index, context=context)

    def assertCountersMatchExpected(
        self, adapted: Adapted, expected: dict, *, context: str
    ) -> None:
        """Assert the adapter's counters against the expected file, key by key.

        Every key is compared, and the key sets are compared too: a counter the expected
        file names and the adapter no longer returns would otherwise pass unnoticed.
        """
        expected_counters = expected["counters"]
        self.assertEqual(
            set(adapted.counters),
            set(expected_counters),
            msg=f"{context}: the counter key set must be exactly the recorded one",
        )
        for key in sorted(expected_counters):
            self.assertEqual(
                adapted.counters[key],
                expected_counters[key],
                msg=(
                    f"{context}: counter {key!r} -- expected {expected_counters[key]!r}, "
                    f"observed {adapted.counters[key]!r}"
                ),
            )

    def assertReconciles(self, adapted: Adapted, expected: dict, *, context: str) -> None:
        """Assert ``raw finding records = rows + rejections`` against both authorities.

        The left-hand side is taken twice from two independent places -- this file's own
        traversal and the expected file's hand-verified number -- and both must agree with
        ``rows + rejections``.
        """
        counts = expected["counts"]
        self.assertEqual(
            adapted.raw_records,
            counts["raw_finding_records"],
            msg=(
                f"{context}: the independent traversal counted {adapted.raw_records} "
                f"records against the expected {counts['raw_finding_records']}"
            ),
        )
        self.assertEqual(
            adapted.raw_records,
            len(adapted.rows) + len(adapted.rejections),
            msg=(
                f"{context}: {adapted.raw_records} records must equal "
                f"{len(adapted.rows)} rows + {len(adapted.rejections)} rejections"
            ),
        )
        self.assertEqual(len(adapted.rows), counts["rows"], msg=f"{context}: row count")
        self.assertEqual(
            len(adapted.rejections), counts["rejections"], msg=f"{context}: rejection count"
        )
        self.assertEqual(
            adapted.by_class,
            {key: value for key, value in counts["rejections_by_class"].items()},
            msg=f"{context}: rejections per named class",
        )

    def assertSchemaClean(self, rows, *, context: str) -> None:
        """Assert the emitted rows satisfy the output schema, through ``emit.py`` itself.

        ``emit.validate_rows`` enforces the twelve fields and the absence convention and
        raises on the first fault; ``emit.validation_summary`` then measures the same rules
        so the assertion is a number rather than the absence of an exception. Using the
        emitter's own rules is deliberate: a second spelling of them here could drift from
        the ones the deliverable files are actually written under.
        """
        emit.validate_rows(rows)
        summary = emit.validation_summary(rows)
        self.assertTrue(
            summary["passed"],
            msg=f"{context}: emit.validation_summary reported {summary['violations']}",
        )
        self.assertEqual(summary["rows"], len(rows), msg=f"{context}: rows measured")
        self.assertEqual(
            summary["rows_with_exactly_twelve_fields"],
            len(rows),
            msg=f"{context}: every row carries exactly twelve fields",
        )
        self.assertEqual(summary["absolute_paths"], 0, msg=f"{context}: no absolute path")
        self.assertEqual(summary["path_absent"], 0, msg=f"{context}: path is never absent")
        self.assertEqual(
            summary["severity_norm_absent"], 0, msg=f"{context}: severity_norm never absent"
        )

    def assertRelativeInsideTree(self, value: str, *, context: str) -> None:
        """Assert one emitted path is root-relative with no ``../`` segment.

        This is the user's worked example expressed as an assertion: the failure it warns
        about produces a long ``../`` chain rather than an error, so both halves are checked
        -- not absolute, and no parent segment.
        """
        self.assertFalse(
            value.startswith("/"), msg=f"{context}: {value!r} must not be absolute"
        )
        self.assertNotIn(
            "../", value, msg=f"{context}: {value!r} must carry no ../ segment"
        )
        self.assertNotEqual(value, "", msg=f"{context}: path is never empty")
        self.assertEqual(
            paths.path_kind_for(value),
            paths.PATH_KIND_TREE_FILE,
            msg=f"{context}: {value!r} must resolve inside the scanned tree",
        )

    # ------------------------------------------------------------ rejection-level asserts

    def assertRejection(self, rejection, expectation: dict, *, context: str) -> None:
        """Assert one rejection against its expectation: class by name, detail, identity.

        The class is checked three ways -- against the expected file's literal, against
        membership in ``paths.REJECT_CLASSES`` and through ``paths.is_reject_class`` -- so a
        class name that is plausible but not one of the closed ten cannot pass.
        """
        expected_class = expectation["reject_class"]
        self.assertIn(
            expected_class,
            paths.REJECT_CLASSES,
            msg=(
                f"{context}: the expected class {expected_class!r} must be one of the ten "
                f"closed members of paths.REJECT_CLASSES"
            ),
        )
        self.assertEqual(
            rejection.reject_class,
            expected_class,
            msg=f"{context}: rejection class -- detail was {rejection.detail!r}",
        )
        self.assertTrue(
            paths.is_reject_class(rejection.reject_class),
            msg=f"{context}: {rejection.reject_class!r} must be a canonical class",
        )
        self.assertEqual(rejection.tool, checkov.TOOL, msg=f"{context}: rejection tool")
        self.assertEqual(
            rejection.detail,
            expectation["expected_detail"],
            msg=f"{context}: the diagnostic must be retained verbatim",
        )
        self.assertEqual(
            dict(rejection.record_identity),
            expectation["expected_record_identity"],
            msg=f"{context}: the rejected record's identifying fields",
        )


# ======================================================================================
# 0. The inputs, before anything is asserted about behaviour
# ======================================================================================


class FixtureInventoryTests(CheckovAdapterTestCase):
    """Every fixture and its expectation is present, well formed and unchanged.

    This class precedes every behavioural one for a specific reason: a fixture silently
    absent, or one whose bytes have drifted from the document its expected rows were
    derived from, would let every assertion below pass over an input nobody verified.

    The inventory is ``ALL_FIXTURES`` and it claims all of them: the capture, the two
    declared derived files -- the array-form counterpart and the feature fixture -- and
    every negative fixture in ``REJECT_FIXTURES``. A fixture this module reads but its
    inventory does not claim would be a fixture whose bytes nothing pins, so the counts
    are taken from the tuples rather than restated here where they could drift from them.
    """

    def test_every_fixture_and_expected_file_is_present(self) -> None:
        """Every fixture in the inventory, and its expected file, exists where it is named."""
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                self.assertTrue(
                    fixture_path(stem).is_file(),
                    msg=f"missing fixture {fixture_path(stem)}",
                )
                self.assertTrue(
                    expected_path(stem).is_file(),
                    msg=f"missing expectation {expected_path(stem)}",
                )

    def test_every_fixture_is_unchanged_by_sha256(self) -> None:
        """Each fixture's bytes are the bytes its expected result was derived from.

        The digest and byte size come from the expected file's own ``fixture`` block. An
        edit to a fixture is then a visible failure here rather than a silent change in what
        every other assertion means -- which is exactly why the positive fixture is required
        to be *unmodified captured output* in the first place.

        What this test does **not** establish is provenance. Both sides of the comparison are
        files this tree owns, so a fixture and its expectation can agree perfectly about a
        document the tool never emitted. Provenance is
        :class:`RawArtifactProvenanceTests`, which opens ``harness/artifacts/raw/checkov.json``
        and compares the captured fixture with the runner's own artifact. The two assertions
        are complementary and neither substitutes for the other.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)["fixture"]
                path = fixture_path(stem)
                self.assertEqual(
                    expected["path"],
                    f"oss-scan-results/adapter-tests/fixtures/{stem}.json",
                    msg=f"{stem}: the expectation must name the fixture it describes",
                )
                self.assertEqual(
                    sha256_of(path), expected["sha256"], msg=f"{stem}: fixture sha256"
                )
                self.assertEqual(
                    path.stat().st_size, expected["bytes"], msg=f"{stem}: fixture byte size"
                )

    def test_every_fixture_parses_and_carries_a_recorded_top_level_shape(self) -> None:
        """Each fixture parses as JSON and is the top-level shape its expectation records."""
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                recorded = load_expected(stem)["fixture"]["top_level_form"]
                self.assertIn(recorded, ("object", "array"), msg=f"{stem}: recorded shape")
                if recorded == "array":
                    self.assertIsInstance(document, list, msg=f"{stem}: array form")
                else:
                    self.assertIsInstance(document, dict, msg=f"{stem}: object form")

    def test_both_top_level_shapes_are_covered_by_a_committed_fixture(self) -> None:
        """Both shapes are covered, and the shape-equivalence pair is one of each.

        AAP 0.5.4 requires both shapes detected and handled. The captured artifact is the
        object form -- a single framework reported over the three Kubernetes Dockerfiles --
        and the array form is the committed derived counterpart, so the multi-framework path
        is exercised against a real document rather than one this test invented. The feature
        fixture is the object form too and makes no shape claim: its job is the buckets the
        artifact does not carry, which is why the pair is asserted separately from the
        inventory of positive fixtures.
        """
        shapes = {
            stem: load_expected(stem)["fixture"]["top_level_form"] for stem in POSITIVE_FIXTURES
        }
        self.assertEqual(
            shapes,
            {
                CAPTURED_FIXTURE: "object",
                ALT_SHAPE_FIXTURE: "array",
                FEATURE_FIXTURE: "object",
            },
            msg="every positive fixture records the shape it is",
        )
        self.assertEqual(
            {shapes[stem] for stem in SHAPE_PAIR},
            {"object", "array"},
            msg="the shape-equivalence pair must be one document of each shape",
        )

    def test_expected_files_agree_with_this_adapter_on_tool_class_and_schema(self) -> None:
        """Every expectation names this tool, its fixed class and the twelve fields in order."""
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)
                self.assertEqual(expected["tool"], checkov.TOOL)
                self.assertEqual(expected["scanner_class"], checkov.SCANNER_CLASS)
                self.assertEqual(expected["field_order"], list(emit.FIELDS))
                self.assertEqual(
                    sorted(expected["absence_permitted_fields"]),
                    sorted(emit.OPTIONAL_FIELDS),
                    msg="absence is permitted for exactly the five optional fields",
                )
                self.assertEqual(
                    expected["adapter"],
                    "harness/lib/normalize/adapters/checkov.py",
                    msg="the expectation must name the adapter it was derived against",
                )

    def test_every_expected_row_is_schema_legal_and_carries_no_absolute_path(self) -> None:
        """The expectations themselves satisfy the output schema.

        Asserted through ``emit.py``'s own rules rather than by eye: an expected row
        carrying an absolute path, a thirteenth field or an absent ``path`` would make a
        passing comparison meaningless.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                rows = load_expected(stem)["rows"]
                self.assertSchemaClean(rows, context=f"{stem} expected rows")
                for index, row in enumerate(rows):
                    self.assertRelativeInsideTree(
                        row["path"], context=f"{stem} expected row {index}"
                    )

    def test_every_expected_rejection_class_is_a_canonical_member(self) -> None:
        """Each expected class is one of the ten closed ``paths.REJECT_CLASSES`` members.

        And each is one of the six this adapter can actually produce. A class name that is
        plausible but not canonical -- or canonical but unreachable from this adapter, such
        as a ``uriBaseId`` fault -- would be a defect in the expectation rather than in the
        adapter, and it would go unnoticed if only the counts were compared.
        """
        for stem in REJECT_FIXTURES:
            with self.subTest(fixture=stem):
                for expectation in load_expected(stem)["rejections"]:
                    reject_class = expectation["reject_class"]
                    self.assertIn(reject_class, paths.REJECT_CLASSES)
                    self.assertTrue(paths.is_reject_class(reject_class))
                    self.assertIn(
                        reject_class,
                        PRODUCIBLE_REJECT_CLASSES,
                        msg=(
                            f"{stem}: {reject_class!r} is not among the classes this "
                            "adapter can produce"
                        ),
                    )
                    self.assertIn(
                        reject_class,
                        paths.REJECT_CLASS_DESCRIPTIONS,
                        msg=f"{stem}: every class carries a description for tool-status.md",
                    )

    def test_every_reject_condition_has_a_committed_fixture(self) -> None:
        """A committed negative fixture for every rejection condition this adapter produces.

        AAP 0.9.4: a negative fixture and assertion for **every** rejection condition this
        adapter can produce, present whether or not this run's own artifact contained the
        case. All six of :data:`PRODUCIBLE_REJECT_CLASSES` are covered by a fixture here,
        ``absent_path`` included -- it is *also* asserted from a constructed record in
        :class:`AnchorReconciliationTests`, and the two are complementary rather than
        alternatives: the fixture proves the class survives a whole committed document,
        the constructed record proves which branch reaches it.

        The assertion is equality against the producible set rather than a subset check.
        A class that becomes producible without a fixture must fail here, and a fixture
        asserting a class this adapter cannot produce is caught by
        :meth:`test_every_expected_rejection_class_is_a_canonical_member`.
        """
        observed = set()
        for stem in REJECT_FIXTURES:
            for expectation in load_expected(stem)["rejections"]:
                observed.add(expectation["reject_class"])
        self.assertEqual(
            observed,
            set(PRODUCIBLE_REJECT_CLASSES),
            msg="the committed negative fixtures must cover every producible class",
        )


class RawArtifactProvenanceTests(unittest.TestCase):
    """The captured fixture *is* the runner's artifact, asserted against the artifact.

    Every other provenance claim in this tree is checkable only against a digest the tree
    itself owns -- the expected file records the fixture's sha256, and
    :meth:`FixtureInventoryTests.test_every_fixture_is_unchanged_by_sha256` compares the
    two. That proves self-consistency and nothing about provenance: a fixture and its
    expected file can agree perfectly about a document the tool never emitted. So this class
    opens ``harness/artifacts/raw/checkov.json`` and compares the fixture with it.

    What "byte for byte" means for a JSON record
    --------------------------------------------
    Two levels are asserted, and the distinction matters. At **file** level the comparison
    is literal: this artifact is 8,380 bytes and needed no excerpting, so the fixture's byte
    size and sha256 must equal the artifact's -- the same bytes, not a faithful selection.
    At **record** level the operative meaning is canonical equality: two JSON objects are
    the same record when ``json.dumps(obj, sort_keys=True)`` agrees, which compares every
    key and every value and is insensitive only to key order and to insignificant
    whitespace. That is the right granularity for a fixture that legitimately reformats an
    excerpt, and it is asserted here alongside the stronger file-level equality so a future
    fixture that *did* excerpt would still be held to the record-level claim.

    The envelope is asserted too, because AAP 0.6.2 requires the enclosing structure
    preserved and Checkov's enclosing structure carries meaning: ``check_type`` names the
    framework, the *set of buckets present in* ``results`` is what makes an absent bucket an
    absence rather than an emptiness, and ``summary`` is the tool's own statement about
    itself. A fixture that filled in a bucket the artifact omits would still map to the same
    rows while misrepresenting what this provisioning's runner emitted -- which is precisely
    the defect this class closes.

    Nothing here writes. ``harness/artifacts/raw/`` is runner-only (AAP 0.8.1), so the
    artifact is opened read-only and the fixtures are compared to it.
    """

    #: The artifact's top-level members, in the artifact's own order.
    ARTIFACT_ENVELOPE_MEMBERS = ("check_type", "results", "summary")

    #: The exact set of buckets the artifact's ``results`` object carries. One bucket: the
    #: runner invokes checkov with ``--compact``, so ``passed_checks``, ``skipped_checks``
    #: and ``parsing_errors`` are absent keys rather than empty arrays. Restated here so the
    #: check is against a named set rather than against whatever the fixture happens to
    #: hold.
    ARTIFACT_RESULTS_BUCKETS = frozenset({"failed_checks"})

    #: The keys one failed-check record carries. The fixture may not introduce a key outside
    #: this set: an adapter that read one would be reading something no artifact carries.
    ARTIFACT_RECORD_KEYS = frozenset(
        {
            "check_id",
            "bc_check_id",
            "check_name",
            "check_result",
            "code_block",
            "file_path",
            "file_abs_path",
            "repo_file_path",
            "file_line_range",
            "resource",
            "evaluations",
            "check_class",
            "fixed_definition",
            "entity_tags",
            "caller_file_path",
            "caller_file_line_range",
            "resource_address",
            "severity",
            "bc_category",
            "benchmarks",
            "description",
            "short_description",
            "vulnerability_details",
            "connected_node",
            "guideline",
            "details",
            "check_len",
            "definition_context_file_path",
        }
    )

    @staticmethod
    def canonical(record: object) -> str:
        """Return one record's canonical form -- the operative meaning of identical."""
        return json.dumps(record, sort_keys=True)

    def raw_document(self) -> dict:
        """Return the runner's artifact, failing by name if it is not there.

        An explicit failure rather than a skip. ``adapter-tests-run.json`` reports the
        suite's skipped count as a property of the run, so a provenance assertion that
        skipped itself when the evidence was missing would be indistinguishable from one
        that passed, and the provenance of the captured fixture would rest on nothing.
        """
        self.assertTrue(
            RAW_ARTIFACT_PATH.is_file(),
            f"{RAW_ARTIFACT_PATH} is missing. The captured fixture's provenance is "
            "asserted against the runner's own artifact, so its absence is a failure "
            "rather than a reason to skip: without it, nothing here establishes that "
            f"fixtures/{CAPTURED_FIXTURE}.json is output this tool produced.",
        )
        document = json.loads(RAW_ARTIFACT_PATH.read_text(encoding="utf-8"))
        self.assertIsInstance(
            document, dict, "this artifact's top level is the single-report object form"
        )
        results = document.get("results")
        self.assertIsInstance(results, dict, "the artifact carries a results object")
        self.assertIsInstance(
            results.get(EMITTED_SECTION), list, "the artifact carries a failed_checks array"
        )
        self.assertGreater(
            len(results[EMITTED_SECTION]),
            0,
            "an artifact with no failed check could not source a positive fixture",
        )
        return document

    def artifact_failed_checks(self) -> list:
        """Return the artifact's ``results.failed_checks`` in the artifact's own order."""
        return self.raw_document()["results"][EMITTED_SECTION]

    def test_the_captured_fixture_is_the_artifact_byte_for_byte(self) -> None:
        """Byte size and sha256, both equal to the artifact's.

        The whole artifact was captured, so this is the strongest form the claim can take.
        Byte size is asserted beside the digest because a size mismatch names the problem
        immediately, while two digests differ opaquely.
        """
        self.raw_document()
        fixture = fixture_path(CAPTURED_FIXTURE)
        self.assertEqual(
            fixture.stat().st_size,
            RAW_ARTIFACT_PATH.stat().st_size,
            f"fixtures/{CAPTURED_FIXTURE}.json must be {RAW_ARTIFACT_PATH} byte for byte",
        )
        self.assertEqual(
            sha256_of(fixture),
            sha256_of(RAW_ARTIFACT_PATH),
            f"fixtures/{CAPTURED_FIXTURE}.json must have the artifact's sha256",
        )

    def test_the_expected_file_records_the_artifacts_digest_and_says_so(self) -> None:
        """The expectation's fixture block states the equality it is entitled to state.

        ``fixtures/checkov.json`` is an unmodified capture of the runner's own artifact, and
        the expected file states that provenance in the terms it can be checked in: the
        digest recorded there is the artifact's, and the equality is claimed explicitly, so
        a captured fixture is distinguishable from a derived one by reading the expected file
        alone. The two authored counts are asserted at zero for the same reason: they are the
        fields that would have to move if anything were ever added to this fixture.
        """
        artifact = self.raw_document()
        recorded = load_expected(CAPTURED_FIXTURE)["fixture"]
        self.assertEqual(recorded["excerpt_of"], "harness/artifacts/raw/checkov.json")
        self.assertEqual(recorded["sha256"], sha256_of(RAW_ARTIFACT_PATH))
        self.assertEqual(recorded["raw_artifact_sha256"], sha256_of(RAW_ARTIFACT_PATH))
        self.assertEqual(recorded["bytes"], RAW_ARTIFACT_PATH.stat().st_size)
        self.assertEqual(recorded["raw_artifact_bytes"], RAW_ARTIFACT_PATH.stat().st_size)
        self.assertIs(recorded["sha256_equals_the_raw_artifact"], True)
        self.assertEqual(recorded["authored_failed_checks"], 0)
        self.assertEqual(recorded["authored_envelope_members"], 0)
        self.assertEqual(
            recorded["captured_verbatim_failed_checks"],
            len(artifact["results"][EMITTED_SECTION]),
        )
        self.assertEqual(recorded["check_type"], artifact["check_type"])
        self.assertEqual(recorded["envelope_members"], list(self.ARTIFACT_ENVELOPE_MEMBERS))
        self.assertEqual(
            sorted(recorded["results_buckets"]), sorted(self.ARTIFACT_RESULTS_BUCKETS)
        )
        self.assertEqual(recorded["summary_as_reported"], artifact["summary"])

    def test_every_captured_failed_check_is_identical_to_the_artifacts(self) -> None:
        """Record for record, in order, under canonical comparison.

        Asserted per element rather than as two lists so a failure names the index, and
        asserted in order because the row order both output files use is the artifact's
        order: a fixture carrying the same records shuffled would map correctly and still
        misrepresent what the tool emitted.
        """
        artifact = self.artifact_failed_checks()
        fixture = load_fixture(CAPTURED_FIXTURE)["results"][EMITTED_SECTION]
        self.assertEqual(
            len(fixture),
            len(artifact),
            "the fixture is the whole artifact, so record for record",
        )
        for index, (captured, original) in enumerate(zip(fixture, artifact)):
            with self.subTest(failed_check=index):
                self.assertEqual(
                    self.canonical(captured),
                    self.canonical(original),
                    f"failed check {index} differs from the artifact's",
                )
                self.assertEqual(
                    list(captured),
                    list(original),
                    f"failed check {index}: key order is preserved too, which is stronger "
                    "than canonical equality and true of this fixture",
                )

    def test_the_envelope_members_and_bucket_set_match_the_artifacts(self) -> None:
        """``check_type``, the exact bucket set in ``results``, and ``summary``.

        The bucket *set* rather than the bucket contents: the artifact omits
        ``passed_checks``, ``skipped_checks`` and ``parsing_errors`` entirely, and a fixture
        that added them as empty arrays -- or filled them -- would be a different document
        about the same findings. ``summary`` is compared canonically because it is the
        tool's own statement about itself and is quoted verbatim into the expected file.
        """
        artifact = self.raw_document()
        fixture = load_fixture(CAPTURED_FIXTURE)
        self.assertIsInstance(fixture, dict)
        self.assertEqual(list(fixture), list(artifact), "top-level members, in order")
        self.assertEqual(list(fixture), list(self.ARTIFACT_ENVELOPE_MEMBERS))
        self.assertEqual(fixture["check_type"], artifact["check_type"])
        self.assertEqual(
            set(fixture["results"]),
            set(artifact["results"]),
            "the set of buckets present in results must be the artifact's",
        )
        self.assertEqual(set(fixture["results"]), set(self.ARTIFACT_RESULTS_BUCKETS))
        self.assertEqual(self.canonical(fixture["summary"]), self.canonical(artifact["summary"]))

    def test_the_fixture_introduces_no_member_the_artifact_lacks(self) -> None:
        """Across every record, the fixture's key union is the artifact's.

        Stated as a set relation over the whole document as well as per record, because a
        fixture with several records could keep each record's key set legal while the union
        grew -- and the union is what an adapter can reach.
        """
        artifact = self.artifact_failed_checks()
        fixture = load_fixture(CAPTURED_FIXTURE)["results"][EMITTED_SECTION]
        artifact_keys: set = set()
        for record in artifact:
            artifact_keys |= set(record)
        fixture_keys: set = set()
        for index, record in enumerate(fixture):
            with self.subTest(failed_check=index):
                self.assertIsInstance(record, dict)
                self.assertEqual(set(record), self.ARTIFACT_RECORD_KEYS)
            fixture_keys |= set(record)
        self.assertEqual(artifact_keys, set(self.ARTIFACT_RECORD_KEYS))
        self.assertEqual(fixture_keys - artifact_keys, set())
        self.assertEqual(fixture_keys, artifact_keys)

    def test_each_derived_fixture_is_declared_derived_and_is_not_the_artifact(self) -> None:
        """A derived fixture says so, and is demonstrably not the capture.

        Both halves are needed. The declaration is what a reader relies on; the inequality
        is what stops the declaration from being decoration. A derived fixture that had
        silently become identical to the artifact would leave the cases it exists for with
        no home while every other assertion in this module still passed.
        """
        artifact_digest = sha256_of(RAW_ARTIFACT_PATH)
        artifact_size = RAW_ARTIFACT_PATH.stat().st_size
        artifact_records = len(self.artifact_failed_checks())
        for stem in DERIVED_FIXTURES:
            with self.subTest(fixture=stem):
                recorded = load_expected(stem)["fixture"]
                self.assertTrue(
                    recorded["provenance"].startswith("derived"),
                    f"{stem}: the expectation must declare the fixture derived, not "
                    f"{recorded['provenance']!r}",
                )
                self.assertTrue(
                    recorded["derived_from"], f"{stem}: what it was derived from is named"
                )
                self.assertNotIn(
                    "excerpt_of",
                    recorded,
                    f"{stem}: a derived fixture makes no captured-excerpt claim",
                )
                self.assertNotIn(
                    "sha256_equals_the_raw_artifact",
                    recorded,
                    f"{stem}: only the capture may claim the artifact's digest",
                )

                path = fixture_path(stem)
                self.assertNotEqual(sha256_of(path), artifact_digest)
                self.assertNotEqual(path.stat().st_size, artifact_size)

        # The two derived fixtures are derived in different ways and each states its own
        # difference from the artifact, so each is held to its own declaration rather than
        # to a shared one.
        alternate = load_expected(ALT_SHAPE_FIXTURE)["fixture"]
        self.assertEqual(
            alternate["derived_from"],
            f"oss-scan-results/adapter-tests/fixtures/{CAPTURED_FIXTURE}.json",
        )
        self.assertEqual(alternate["derived_from_sha256"], artifact_digest)
        self.assertTrue(alternate["transformation"])
        self.assertTrue(alternate["shape_only_assertion"])
        self.assertTrue(alternate["not_a_capture"])
        self.assertIsInstance(load_fixture(ALT_SHAPE_FIXTURE), list)

        features = load_expected(FEATURE_FIXTURE)["fixture"]
        self.assertEqual(features["provenance"], "derived")
        self.assertTrue(features["provenance_statement"])
        self.assertTrue(features["feature_cases_this_fixture_exists_to_exercise"])
        against = features["not_identical_to_the_raw_artifact"]
        self.assertEqual(against["raw_artifact"], "harness/artifacts/raw/checkov.json")
        self.assertEqual(against["raw_artifact_sha256"], artifact_digest)
        self.assertEqual(against["raw_artifact_bytes"], artifact_size)
        self.assertEqual(against["raw_artifact_failed_checks"], artifact_records)
        self.assertEqual(
            sorted(against["raw_artifact_results_buckets"]),
            sorted(self.ARTIFACT_RESULTS_BUCKETS),
        )
        self.assertNotEqual(
            set(load_fixture(FEATURE_FIXTURE)["results"]),
            set(self.ARTIFACT_RESULTS_BUCKETS),
            "the feature fixture exists because it carries buckets the artifact does not",
        )

    def test_the_retained_records_of_the_feature_fixture_are_the_artifacts(self) -> None:
        """Its five failed checks are five of the artifact's six, unedited and in order.

        The feature fixture is derived, so its file-level bytes differ by design -- but the
        records it retained are not authored either, and that is worth asserting rather than
        describing: it is why the rows it produces are rows about real locations in the
        pinned tree. The comparison is canonical and whole-object, so an edited field fails
        here.
        """
        artifact_records = self.artifact_failed_checks()
        artifact = {self.canonical(record) for record in artifact_records}
        retained = load_fixture(FEATURE_FIXTURE)["results"][EMITTED_SECTION]
        self.assertEqual(
            len(retained),
            load_expected(FEATURE_FIXTURE)["fixture"][EMITTED_SECTION],
            "the count its own expectation records",
        )
        self.assertLess(
            len(retained),
            len(artifact_records),
            "one of the artifact's failures was relocated into skipped_checks, which is "
            "what gives the skipped-check case a real record at a real location",
        )
        for index, record in enumerate(retained):
            with self.subTest(failed_check=index):
                self.assertIn(
                    self.canonical(record),
                    artifact,
                    f"failed check {index} of the feature fixture is not one of the "
                    "artifact's records unedited",
                )


class AdapterContractTests(CheckovAdapterTestCase):
    """The constants this adapter publishes, checked against the modules that own them."""

    def test_the_tool_identifier_and_scanner_class_are_fixed(self) -> None:
        """``checkov`` and ``misconfig``, the latter fixed by AAP 0.5.4's class table.

        ``misconfig`` never varies -- not by ``check_type``, not by framework, not by record
        content. Only ``trivy`` varies, and it varies by the section a record came from.
        """
        self.assertEqual(checkov.TOOL, "checkov")
        self.assertEqual(checkov.SCANNER_CLASS, "misconfig")
        self.assertIn(checkov.TOOL, severity.CANONICAL_TOOLS)

    def test_the_authored_field_list_agrees_with_the_emitter(self) -> None:
        """The adapter's ``FIELDS`` copy equals ``emit.FIELDS`` exactly, in order.

        AAP 0.6.4 permits an adapter to import ``paths`` and ``severity`` and nothing else,
        so ``emit.FIELDS`` cannot be imported there and the adapter's copy is authored. That
        makes agreement something to assert rather than something the type system provides.
        """
        self.assertEqual(checkov.FIELDS, emit.FIELDS)
        self.assertEqual(len(checkov.FIELDS), 12)
        self.assertEqual(
            checkov.ABSENCE_PERMITTED_FIELDS,
            frozenset(emit.OPTIONAL_FIELDS),
            msg="absence is permitted for exactly the five optional fields",
        )
        self.assertNotIn("path", checkov.ABSENCE_PERMITTED_FIELDS)
        self.assertNotIn("severity_norm", checkov.ABSENCE_PERMITTED_FIELDS)

    def test_the_emitted_and_never_emitted_sections_are_the_named_buckets(self) -> None:
        """``failed_checks`` is the only emitted bucket; passes and skips are never emitted.

        This is what ties the independent traversal in this file to the adapter: the
        traversal walks the literal ``failed_checks``, and this assertion is what says the
        adapter walks the same bucket. A rename on either side then fails here rather than
        moving both sides together and making the counts agree for the wrong reason.
        """
        self.assertEqual(checkov.EMITTED_RESULT_SECTION, EMITTED_SECTION)
        self.assertEqual(checkov.NEVER_EMITTED_RESULT_SECTIONS, NEVER_EMITTED_SECTIONS)
        self.assertNotIn(EMITTED_SECTION, checkov.NEVER_EMITTED_RESULT_SECTIONS)

    def test_new_counters_returns_every_key_at_zero(self) -> None:
        """A fresh counter mapping carries all of ``COUNTER_KEYS`` and every value is zero.

        Fully pre-initialised matters downstream: a caller aggregating several artifacts adds
        them, so a missing key would be ambiguous between "zero" and "not measured".
        """
        counters = checkov.new_counters()
        self.assertEqual(tuple(counters), checkov.COUNTER_KEYS)
        self.assertEqual(set(counters.values()), {0})
        for key in checkov.COUNTER_KEYS:
            self.assertIsInstance(counters[key], int, msg=f"{key} must be an int")

    def test_the_four_reported_counters_and_both_derived_vocabularies_are_present(self) -> None:
        """The counters AAP 0.5.4 has reported per tool, and one key per vocabulary member.

        The four are ``multi_location_records``, ``multi_valued_cwe_records``,
        ``multi_valued_cve_records`` and ``non_filesystem_paths``. Deriving the other two
        families from ``paths.PATH_KINDS`` and ``severity.BASIS_VALUES`` is what stops the
        counter set from drifting from the vocabularies it reports against.
        """
        counters = checkov.new_counters()
        for key in (
            "multi_location_records",
            "multi_valued_cwe_records",
            "multi_valued_cve_records",
            "non_filesystem_paths",
        ):
            self.assertIn(key, counters, msg=f"{key} is reported per tool")
        for kind in paths.PATH_KINDS:
            self.assertIn(f"{checkov.COUNTER_PATH_KIND_PREFIX}{kind}", counters)
        for basis in severity.BASIS_VALUES:
            self.assertIn(f"{checkov.COUNTER_SEVERITY_BASIS_PREFIX}{basis}", counters)


class EnvironmentContractTests(CheckovAdapterTestCase):
    """The hermetic inputs are the authoritative ones, loaded through ``paths.py``."""

    def test_the_written_allowlist_is_the_twelve_authoritative_globs(self) -> None:
        """Twelve globs, byte-exact, in the request's order, with no exclusion line.

        Written by this module and read back through ``paths.load_allowlist``, then checked
        against ``paths.allowlist_matches_authoritative_globs`` -- so the file is the
        authority and the module's own copy is the independent opinion, rather than the
        module being compared with itself.
        """
        for environment in (recorded_env(), tree_env(), single_target_env()):
            with self.subTest(environment=environment.name):
                self.assertEqual(environment.globs, AUTHORITATIVE_GLOBS)
                self.assertEqual(len(environment.globs), 12)
                self.assertTrue(
                    paths.allowlist_matches_authoritative_globs(environment.globs),
                    msg="the loaded globs must be the ones paths.py authors",
                )
                for glob in environment.globs:
                    self.assertNotIn(
                        paths.SRC_TEST_MARKER,
                        glob,
                        msg="the src/test exclusion lives in paths.py, not the allowlist",
                    )

    def test_the_written_allowlist_matches_the_runs_own_allowlist_digest(self) -> None:
        """The twelve lines this module writes are byte-identical to the run's scope file.

        ``harness/scope/allowlist.txt`` carries sha256
        ``0013edf6cdc3a48d69aed5d7db41cc6647cfd461d348f5e1d563ba85664143d1``, and every
        expected file records that digest in its ``allowlist`` block. Asserting the digest of
        the file written here is what makes "the twelve authoritative globs" a checkable
        claim rather than a transcription anyone has to trust.
        """
        recorded_digest = load_expected(CAPTURED_FIXTURE)["allowlist"]["sha256"]
        for environment in (recorded_env(), tree_env(), single_target_env()):
            with self.subTest(environment=environment.name):
                self.assertEqual(
                    sha256_of(environment.allowlist_path),
                    recorded_digest,
                    msg="the written allowlist must be byte-identical to the run's",
                )

    def test_the_recorded_path_base_reaches_the_adapter_as_the_metadata_records_it(self) -> None:
        """The ``ToolPathBase`` comes from a written document through ``paths.py``'s loader.

        Not constructed directly, because the loader is the route ``cli.py`` takes and the
        base is a property of how the runner was invoked rather than of an artifact.
        """
        base = recorded_env().tool_base
        self.assertIsInstance(base, paths.ToolPathBase)
        self.assertEqual(base.tool, checkov.TOOL)
        self.assertEqual(base.kind, RECORDED_PATH_BASE_KIND)
        self.assertEqual(base.anchor_fields, RECORDED_ANCHOR_FIELDS)
        self.assertEqual(base.base_value, RECORDED_SCAN_ROOT)
        self.assertEqual(base.scan_root, RECORDED_SCAN_ROOT)
        self.assertTrue(base.has_explicit_base)
        self.assertEqual(base.invocations_per_run, RECORDED_INVOCATIONS_PER_RUN)
        self.assertEqual(paths.metadata_scan_root(recorded_env().metadata), RECORDED_SCAN_ROOT)

    def test_the_single_target_environment_records_the_scan_root_kind(self) -> None:
        """The one configuration in which a bare ``file_path`` resolves rather than rejects."""
        base = single_target_env().tool_base
        self.assertEqual(base.kind, paths.PATH_BASE_KIND_SCAN_ROOT)
        self.assertEqual(base.anchor_fields, ())
        self.assertEqual(base.base_for_relative(), RECORDED_SCAN_ROOT)


class RecordedMetadataAgreementTests(CheckovAdapterTestCase):
    """The minimal metadata written here mirrors the run's own record for this tool.

    ``harness/artifacts/logs/runner-metadata.json`` is the document the normalizer reads as
    input (AAP 0.6.4), and it is a declared dependency of this file. Asserting the four
    facts this module restates against it is what stops the hermetic environment from
    quietly testing a configuration the run never had -- and it reads the record only,
    never a rendered document such as ``tool-status.md``.
    """

    def recorded_entry(self):
        """Return ``(tool_base, document)`` from the run's own runner metadata."""
        self.assertTrue(
            RECORDED_METADATA_PATH.is_file(),
            msg=f"the run's runner metadata is a declared dependency: {RECORDED_METADATA_PATH}",
        )
        document = paths.load_runner_metadata(RECORDED_METADATA_PATH)
        return paths.tool_path_base(document, checkov.TOOL), document

    def test_the_recorded_scan_root_is_the_root_these_expectations_were_derived_under(self) -> None:
        """``spark_src`` in the record equals the root the expected counters assume."""
        _, document = self.recorded_entry()
        self.assertEqual(paths.metadata_scan_root(document), RECORDED_SCAN_ROOT)

    def test_the_recorded_base_kind_and_anchor_order_are_the_ones_restated_here(self) -> None:
        """``per_target_directory`` with ``[repo_file_path, file_abs_path]``, in that order.

        The order is load-bearing rather than cosmetic: it decides which anchor a row's path
        is taken from where both are present and they disagree, and the disagreement is
        recorded rather than resolved.
        """
        base, _ = self.recorded_entry()
        self.assertEqual(base.kind, RECORDED_PATH_BASE_KIND)
        self.assertEqual(base.anchor_fields, RECORDED_ANCHOR_FIELDS)
        self.assertEqual(base.tool, checkov.TOOL)

    def test_the_record_states_one_invocation_carrying_many_targets(self) -> None:
        """One invocation, many ``-d`` roots -- the fact that makes ``file_path`` ambiguous.

        With several target roots in a single invocation a record's slash-stripped
        ``file_path`` is relative to whichever root matched, which is precisely why an anchor
        field is required and why a strip-and-join against the tree root is wrong even once
        the leading slash is handled correctly.
        """
        base, _ = self.recorded_entry()
        self.assertEqual(base.invocations_per_run, RECORDED_INVOCATIONS_PER_RUN)
        self.assertEqual(base.working_directory_path, RECORDED_SCAN_ROOT)


# ======================================================================================
# 1. Both top-level shapes (AAP 0.5.4; brief assertions 1-5)
# ======================================================================================


class BothShapesTests(CheckovAdapterTestCase):
    """Each committed shape maps correctly against its own hand-verified expectation."""

    def test_the_captured_object_form_maps_to_its_expected_rows(self) -> None:
        """The unmodified captured artifact excerpt, asserted row by row and counter by counter.

        This is the positive mapping assertion: a hand-written fixture would test the adapter
        against the shape one believed Checkov emits, so the fixture is captured output and
        the expectation was derived from it by hand rather than from a run of the adapter.
        """
        adapted = self.adapt_fixture(CAPTURED_FIXTURE)
        expected = load_expected(CAPTURED_FIXTURE)
        self.assertRowsMatchExpected(adapted, expected, context="captured object form")
        self.assertCountersMatchExpected(adapted, expected, context="captured object form")
        self.assertReconciles(adapted, expected, context="captured object form")
        self.assertSchemaClean(adapted.rows, context="captured object form")
        self.assertEqual(adapted.rejections, [], msg="the positive fixture rejects nothing")

    def test_the_derived_array_form_maps_to_its_expected_rows(self) -> None:
        """The multi-framework form, asserted the same way and against its own expectation.

        Its first report object is a ``kubernetes`` report with every bucket empty: a
        framework that ran and matched nothing. It must still be walked -- ``reports`` is 2 --
        and it must move none of the three container-shape counters, because an empty array
        is still an array.
        """
        adapted = self.adapt_fixture(ALT_SHAPE_FIXTURE)
        expected = load_expected(ALT_SHAPE_FIXTURE)
        self.assertRowsMatchExpected(adapted, expected, context="derived array form")
        self.assertCountersMatchExpected(adapted, expected, context="derived array form")
        self.assertReconciles(adapted, expected, context="derived array form")
        self.assertSchemaClean(adapted.rows, context="derived array form")
        self.assertEqual(adapted.counters["reports"], 2)
        self.assertEqual(adapted.counters["reports_skipped_non_mapping"], 0)
        self.assertEqual(adapted.counters["reports_without_results_object"], 0)
        self.assertEqual(
            adapted.counters["reports_without_failed_checks_array"],
            0,
            msg="an empty failed_checks array is still an array",
        )

    def test_the_shape_is_recorded_from_the_document_and_not_from_a_hint(self) -> None:
        """Detection is on the document: the adapter is never told which shape it was given.

        ``adapt`` takes the tool, the root, the base, the allowlist and the tally, and none of
        them names a shape. The pair of ``top_level_form_*`` counters is what records the
        shape actually observed -- exactly one of the pair is 1 -- which is the whole point of
        a shape that varies with content rather than with the invocation.
        """
        for stem, array_expected in ((CAPTURED_FIXTURE, 0), (ALT_SHAPE_FIXTURE, 1)):
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertEqual(adapted.counters["top_level_form_array"], array_expected)
                self.assertEqual(
                    adapted.counters["top_level_form_object"], 1 - array_expected
                )
                self.assertEqual(
                    adapted.counters["top_level_form_array"]
                    + adapted.counters["top_level_form_object"],
                    1,
                    msg="exactly one of the pair records the shape observed",
                )

    def test_an_empty_report_in_either_shape_yields_nothing_and_raises_nothing(self) -> None:
        """Zero rows, zero rejections, no error -- in both shapes and in four empty forms.

        A framework that matched nothing is not an error condition, and neither is an
        artifact whose only report object is empty. The counters still record what was walked,
        so "nothing to do" is observable rather than indistinguishable from a shape the
        adapter failed to read.
        """
        cases = {
            "object form, empty buckets": object_form(),
            "object form, failed_checks absent": {"check_type": "dockerfile", "results": {}},
            "array form, no elements": array_form(),
            "array form, one empty report": array_form(report()),
            "array form, two empty reports": array_form(
                report(check_type="kubernetes"), report()
            ),
        }
        for label, document in cases.items():
            with self.subTest(document=label):
                adapted = self.adapt(document)
                self.assertEqual(adapted.rows, [], msg=f"{label}: no rows")
                self.assertEqual(adapted.rejections, [], msg=f"{label}: no rejections")
                self.assertEqual(adapted.raw_records, 0, msg=f"{label}: no records")
                self.assertEqual(
                    adapted.counters["rows_in_scope"] + adapted.counters["rows_out_of_scope"],
                    0,
                    msg=f"{label}: the in_scope decomposition sums to the row count",
                )

    def test_a_report_whose_results_container_is_wrong_contributes_nothing(self) -> None:
        """The container shapes that yield no record are counted, never rejected.

        A non-object report element, a ``results`` that is not an object and a
        ``failed_checks`` that is not an array each contribute zero -- and are counted under
        their own key rather than passed over in silence. A rejection would put them on the
        right-hand side of ``rows + rejections`` while the independent traversal never put
        them on the left, breaking the identity in the direction hardest to notice.
        """
        cases = {
            "results is not an object": (
                array_form(report(results="not an object")),
                "reports_without_results_object",
            ),
            "failed_checks is not an array": (
                array_form(report(results={EMITTED_SECTION: "not an array"})),
                "reports_without_failed_checks_array",
            ),
            "a report element is not an object": (
                array_form("not a report object"),
                "reports_skipped_non_mapping",
            ),
        }
        for label, (document, counter) in cases.items():
            with self.subTest(document=label):
                adapted = self.adapt(document)
                self.assertEqual(adapted.rows, [], msg=f"{label}: no rows")
                self.assertEqual(
                    adapted.rejections, [], msg=f"{label}: counted rather than rejected"
                )
                self.assertEqual(adapted.counters[counter], 1, msg=f"{label}: {counter}")
                self.assertEqual(
                    adapted.raw_records,
                    0,
                    msg=f"{label}: the independent traversal contributes zero too",
                )


class SourceDocumentEqualityTests(unittest.TestCase):
    """The two committed documents carry the same ``failed_checks``, compared as documents.

    :class:`ShapeEquivalenceTests` compares what the adapter *made* of the two fixtures --
    rows, counters and the digests the two expectations record about each other. All of that
    can stay green while the promise the alt-shape fixture makes about itself is broken,
    because every field the adapter does not read is invisible to a row comparison:
    ``check_result``, ``code_block``, ``resource``, ``evaluations``, ``guideline``,
    ``severity``, ``benchmarks`` and twenty others. An edit to any of them would leave the
    file claiming to be a shape-only transformation of the capture while no longer being one.

    So this class compares the two source documents directly, and does three things
    deliberately:

    * **No adapter is invoked.** Nothing here imports a row, a counter or a rejection. The
      extraction is this module's own :func:`failed_check_union`, which walks
      ``results.failed_checks[]`` -- ``results.failed_checks`` for the object form, and the
      union across every element in document order for the array form -- and builds nothing.
    * **Whole objects, unprojected and unnormalised.** The records are compared as they are
      committed, not reduced to the fields a row uses. Projecting first would reintroduce
      exactly the blind spot this class exists to remove.
    * **An ordered sequence, never a set.** A set comparison would pass on a document whose
      records had been reordered, and the row *order* both output files use is that order.
      The ordered comparison is asserted first element by element, so a failure names the
      index, and then as whole sequences, so a length or ordering difference cannot hide
      behind a ``zip`` that stopped early.
    """

    @staticmethod
    def canonical(record: object) -> str:
        """Return one record's canonical form -- the operative meaning of identical."""
        return json.dumps(record, sort_keys=True)

    def documents(self) -> tuple:
        """Return the object form's records and the array form's, both in document order."""
        object_form_records = failed_check_union(load_fixture(CAPTURED_FIXTURE))
        array_form_records = failed_check_union(load_fixture(ALT_SHAPE_FIXTURE))
        return object_form_records, array_form_records

    def test_the_extraction_really_reaches_both_documents_records(self) -> None:
        """State what is being compared before comparing it.

        Two empty lists are equal, so the comparison below would pass over a pair of
        documents carrying nothing at all. The counts are additionally required to equal the
        hand-verified ``counts.raw_finding_records`` in each expectation, so the traversal
        used here is pinned to the number the expected files were derived against rather
        than to itself.
        """
        object_form_records, array_form_records = self.documents()
        self.assertGreater(
            len(object_form_records), 0, "the object form must carry failed checks to compare"
        )
        self.assertGreater(
            len(array_form_records), 0, "the array form must carry failed checks to compare"
        )
        self.assertEqual(
            len(object_form_records),
            load_expected(CAPTURED_FIXTURE)["counts"]["raw_finding_records"],
        )
        self.assertEqual(
            len(array_form_records),
            load_expected(ALT_SHAPE_FIXTURE)["counts"]["raw_finding_records"],
        )
        self.assertGreater(
            len(load_fixture(ALT_SHAPE_FIXTURE)),
            1,
            "the array form must carry more than one report, or the union across report "
            "objects is not exercised by this pair at all",
        )

    def test_the_two_documents_failed_checks_are_the_same_records_in_the_same_order(
        self,
    ) -> None:
        """Element for element, as whole objects, before any adapter runs.

        This is the assertion that makes "a shape transformation and nothing else" a
        measurement. Canonical equality is asserted per index so a failure names the record,
        and the two ordered sequences are then compared outright: that catches a length
        difference, a reordering and an extra record, none of which the per-index loop alone
        would catch.
        """
        object_form_records, array_form_records = self.documents()
        self.assertEqual(
            len(object_form_records),
            len(array_form_records),
            "the array form must carry exactly the object form's failed checks",
        )
        for index, (left, right) in enumerate(zip(object_form_records, array_form_records)):
            with self.subTest(failed_check=index):
                self.assertEqual(
                    self.canonical(left),
                    self.canonical(right),
                    f"failed check {index} differs between the two committed documents; a "
                    "field the adapter ignores was edited, so the alt-shape fixture is no "
                    "longer a shape-only transformation of the capture",
                )
                self.assertEqual(
                    list(left), list(right), f"failed check {index}: key order too"
                )
        self.assertEqual(
            object_form_records,
            array_form_records,
            "the ordered sequences must be equal outright, not merely pairwise up to the "
            "shorter of the two",
        )
        self.assertEqual(
            [self.canonical(record) for record in object_form_records],
            [self.canonical(record) for record in array_form_records],
            "asserted as an ordered sequence and never as a set: a reordering must fail",
        )

    def test_the_comparison_would_catch_a_reordering(self) -> None:
        """The ordered comparison is order-sensitive, demonstrated rather than asserted.

        A reader has to be able to tell an ordered comparison from one that happens to be
        written over ordered inputs. This reverses a copy of the extracted records -- the
        committed files are untouched -- and requires the sequence comparison to fail while
        the multiset of records is unchanged. Without this, "ordered multiset, not a set"
        would be a comment rather than a property.
        """
        object_form_records, _ = self.documents()
        self.assertGreater(
            len(object_form_records), 1, "an order claim needs at least two records"
        )
        shuffled = list(reversed(object_form_records))
        self.assertNotEqual(
            [self.canonical(record) for record in object_form_records],
            [self.canonical(record) for record in shuffled],
            "the ordered comparison must distinguish these two sequences",
        )
        self.assertEqual(
            sorted(self.canonical(record) for record in object_form_records),
            sorted(self.canonical(record) for record in shuffled),
            "while the multiset of records is the same, which is exactly what a set "
            "comparison would have accepted",
        )

    def test_the_comparison_would_catch_an_edit_to_a_field_no_row_reads(self) -> None:
        """The blind spot this class closes, demonstrated on a copy rather than described.

        A row carries twelve fields; a failed-check record carries twenty-eight. The
        difference -- ``check_result``, ``code_block``, ``resource``, ``evaluations``,
        ``guideline``, ``benchmarks`` and the rest -- is invisible to any comparison of rows,
        counters or digests, which is exactly why an edit there could break the shape-only
        promise with every other assertion in this module still green.

        This edits one such field on an in-memory deep copy of the extracted records -- the
        committed files are opened read-only and never written -- and requires the whole-object
        comparison to reject it while a comparison projected onto the row's own field sources
        would not have. The projection is spelled out here rather than taken from the adapter:
        the point is what a row-level comparison *can* see, not what this adapter happens to
        read.
        """
        object_form_records, _ = self.documents()
        edited = json.loads(json.dumps(object_form_records))
        target = edited[0]
        self.assertIn(
            "guideline",
            target,
            "the record must carry a field outside the row's twelve for this to mean anything",
        )
        target["guideline"] = "https://example.invalid/edited-by-this-assertion"

        self.assertNotEqual(
            [self.canonical(record) for record in object_form_records],
            [self.canonical(record) for record in edited],
            "the whole-object comparison must reject an edit to a field no row reads",
        )

        def projected(records: list) -> list:
            """Reduce each record to the fields a row is built from."""
            return [
                (
                    record.get("check_id"),
                    record.get("check_name"),
                    record.get("severity"),
                    record.get("file_path"),
                    record.get("file_abs_path"),
                    record.get("repo_file_path"),
                    tuple(record.get("file_line_range") or ()),
                )
                for record in records
            ]

        self.assertEqual(
            projected(object_form_records),
            projected(edited),
            "while a row-level comparison sees no difference at all -- which is the gap this "
            "class exists to close",
        )

    def test_the_added_report_contributes_no_record(self) -> None:
        """The only material the transformation added is one report that carries nothing.

        The array form has to be multi-report or the union across report objects is not
        exercised, so the transformation added one empty framework report. That addition is
        legitimate only while it contributes no record: the report inventory in the
        expectation says so, and this measures it from the document.
        """
        reports = load_fixture(ALT_SHAPE_FIXTURE)
        self.assertIsInstance(reports, list)
        contributed = [len(failed_check_union(element)) for element in reports]
        self.assertEqual(
            sum(contributed),
            len(failed_check_union(load_fixture(CAPTURED_FIXTURE))),
            "the union across reports is the capture's failed checks and nothing more",
        )
        self.assertIn(
            0,
            contributed,
            "one report contributes nothing -- the empty-member element the union is "
            "asserted to be insensitive to",
        )
        recorded = load_expected(ALT_SHAPE_FIXTURE)["fixture"]["report_inventory"]
        self.assertEqual(len(recorded), len(reports))
        for index, entry in enumerate(recorded):
            with self.subTest(report=index):
                self.assertEqual(entry["report_index"], index)
                self.assertEqual(entry["check_type"], reports[index]["check_type"])
                self.assertEqual(entry["contributes_records"], contributed[index])

    def test_the_two_expectations_name_each_other_and_the_documents_they_describe(
        self,
    ) -> None:
        """Each invariant block names the counterpart fixture and its current digest.

        The digests are the tie between this document-level comparison and the row-level one:
        a pair that agreed about records while one expectation described a file that had
        since changed would be agreeing about the wrong thing.
        """
        captured = load_expected(CAPTURED_FIXTURE)["alt_shape_invariant"]
        alternate = load_expected(ALT_SHAPE_FIXTURE)["object_shape_invariant"]
        self.assertEqual(
            captured["counterpart_fixture"],
            f"oss-scan-results/adapter-tests/fixtures/{ALT_SHAPE_FIXTURE}.json",
        )
        self.assertEqual(
            captured["counterpart_fixture_sha256"], sha256_of(fixture_path(ALT_SHAPE_FIXTURE))
        )
        self.assertTrue(captured["shape_only_transformation"])
        self.assertEqual(
            alternate["counterpart_fixture"],
            f"oss-scan-results/adapter-tests/fixtures/{CAPTURED_FIXTURE}.json",
        )
        self.assertEqual(
            alternate["counterpart_fixture_sha256"], sha256_of(fixture_path(CAPTURED_FIXTURE))
        )
        self.assertIs(alternate["counterpart_is_the_raw_artifact"], True)
        self.assertTrue(alternate["source_documents_compared_directly"])


class ShapeEquivalenceTests(CheckovAdapterTestCase):
    """The same records in either shape produce identical rows -- brief assertion 3.

    This is the assertion that proves shape handling is *normalization* rather than two
    divergent code paths, and it is the reason both fixtures are committed: it could not be
    asserted of two forks of the record loop. The alt-shape fixture is a committed file
    derived from the capture by shape transformation alone, never an in-test transformation
    of the captured one -- a transformation performed here would be testing this file's
    arithmetic rather than the adapter's.

    What this class does **not** establish is that the two documents carry the same records:
    everything here is downstream of the adapter, so a field the adapter never reads could
    drift between the two fixtures with every assertion below still passing. That is
    :class:`SourceDocumentEqualityTests`, which compares the two committed ``failed_checks``
    documents directly, as whole objects and in order, before any adapter is invoked. The
    two are complementary -- same records mapped identically -- and neither substitutes for
    the other.
    """

    def test_the_two_shapes_emit_identical_rows_in_identical_order(self) -> None:
        """Field for field, row for row, in order -- not merely set-equal.

        Order equality is the stronger claim and it is checkable from the two fixtures alone:
        the array form's first element contributes no record and its second element's
        ``failed_checks`` are element-for-element identical to the object form's, so the
        emission sequence is that sequence unchanged.
        """
        object_rows = self.adapt_fixture(CAPTURED_FIXTURE).rows
        array_rows = self.adapt_fixture(ALT_SHAPE_FIXTURE).rows
        self.assertEqual(
            len(object_rows), len(array_rows), msg="both shapes emit the same row count"
        )
        for index, (left, right) in enumerate(zip(object_rows, array_rows)):
            self.assertRowFields(left, right, row_index=index, context="shape equivalence")
        self.assertEqual(object_rows, array_rows, msg="the row sequences are identical")

    def test_only_the_three_shape_dependent_counters_differ(self) -> None:
        """Every other counter is identical, because the records are the same records.

        The shape decides how many containers were walked and nothing else, so
        ``top_level_form_array``, ``top_level_form_object`` and ``reports`` are the only keys
        entitled to differ. A fourth key differing would mean the two shapes took different
        decisions about identical content.
        """
        shape_dependent = {"top_level_form_array", "top_level_form_object", "reports"}
        object_counters = self.adapt_fixture(CAPTURED_FIXTURE).counters
        array_counters = self.adapt_fixture(ALT_SHAPE_FIXTURE).counters
        differing = {
            key
            for key in object_counters
            if object_counters[key] != array_counters[key]
        }
        self.assertEqual(
            differing,
            shape_dependent,
            msg=(
                "only the shape-dependent counters may differ; observed "
                f"{sorted(differing)}"
            ),
        )
        self.assertEqual(object_counters["reports"], 1)
        self.assertEqual(array_counters["reports"], 2)

    def test_the_two_expectations_record_the_same_rows(self) -> None:
        """The two hand-verified files agree with each other, independently of the adapter.

        Both expectations state the invariant explicitly -- ``alt_shape_invariant`` on one
        side and ``object_shape_invariant`` on the other -- and each records the counterpart
        fixture's digest. Checking the digests too is what stops the pair from agreeing about
        a fixture one of them no longer describes.
        """
        captured = load_expected(CAPTURED_FIXTURE)
        alternate = load_expected(ALT_SHAPE_FIXTURE)
        self.assertEqual(captured["rows"], alternate["rows"])
        self.assertEqual(
            captured["alt_shape_invariant"]["counterpart_fixture_sha256"],
            sha256_of(fixture_path(ALT_SHAPE_FIXTURE)),
        )
        self.assertEqual(
            alternate["object_shape_invariant"]["counterpart_fixture_sha256"],
            sha256_of(fixture_path(CAPTURED_FIXTURE)),
        )
        self.assertTrue(alternate["object_shape_invariant"]["rows_equal_counterpart"])
        self.assertTrue(alternate["object_shape_invariant"]["row_order_equal"])

    def test_synthetic_records_are_shape_independent_too(self) -> None:
        """The invariant holds for content the fixtures do not carry.

        The committed pair pins the invariant for the captured records; this pins it for
        records built here, so the equivalence is a property of the reduction rather than of
        one document. The array form used is built from named records rather than by
        transforming a fixture.
        """
        root = recorded_env().root
        records = [
            anchored_check(SPARK_DOCKERFILE, root, file_line_range=[36, 48]),
            anchored_check(PYTHON_DOCKERFILE, root, check_id="CKV_DOCKER_9"),
            anchored_check(PYSPARK_TEST_MODULE, root, file_line_range=[17, 17]),
        ]
        as_object = self.adapt(object_form(*records))
        as_array = self.adapt(array_form(report(check_type="kubernetes"), report(*records)))
        self.assertEqual(as_object.rows, as_array.rows)
        self.assertEqual(as_object.rejections, as_array.rejections)
        self.assertEqual(as_object.raw_records, as_array.raw_records)


class RootDependenceTests(CheckovAdapterTestCase):
    """Resolution is string arithmetic against the recorded root, not a filesystem lookup.

    Two things are established here, and both are stated in ``checkov.rows.json``'s
    ``counters_root_note`` rather than being discovered: the rows do not depend on which
    absolute root the adapter is given, and the one counter that does is
    ``path_corroboration_recorded`` -- because an absolute ``file_abs_path`` naming another
    root then relativizes to a ``../`` chain and disagrees with ``repo_file_path``, which is
    recorded rather than resolved.
    """

    def test_rows_are_byte_identical_against_a_materialised_temporary_tree(self) -> None:
        """The same fixture, a different absolute root, and the same rows.

        The tree environment materialises exactly the paths these tests reference, so the
        files genuinely exist under it -- and the rows are unchanged, which is what says
        resolution never consults the filesystem. It is also why a row's ``path`` may
        legitimately name something that is not a file on disk.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                recorded = self.adapt_fixture(stem, env=recorded_env())
                against_tree = self.adapt_fixture(stem, env=tree_env())
                self.assertEqual(
                    recorded.rows,
                    against_tree.rows,
                    msg=f"{stem}: rows must not depend on which absolute root is given",
                )
                self.assertEqual(
                    [rejection.reject_class for rejection in recorded.rejections],
                    [rejection.reject_class for rejection in against_tree.rejections],
                    msg=f"{stem}: nor may the rejection classes",
                )

    def test_only_the_corroboration_counter_moves_with_the_root(self) -> None:
        """Against another root the anchors disagree, and the disagreement is *counted*.

        One recorded disagreement per record and a row kept for every one of them -- the row
        is never suppressed (AAP 0.5.3), which is why the counter is compared against the row
        count rather than against a literal. Every other counter is unchanged, which is what
        makes the recorded counters in the expected files interpretable: they were measured
        under the recorded scan root, and this is the single value that would differ
        elsewhere.
        """
        recorded = self.adapt_fixture(CAPTURED_FIXTURE, env=recorded_env())
        against_tree = self.adapt_fixture(CAPTURED_FIXTURE, env=tree_env())
        self.assertEqual(recorded.counters["path_corroboration_recorded"], 0)
        self.assertEqual(
            against_tree.counters["path_corroboration_recorded"],
            len(against_tree.rows),
            msg="every record's anchors disagree once the root moves",
        )
        differing = {
            key
            for key in recorded.counters
            if recorded.counters[key] != against_tree.counters[key]
        }
        self.assertEqual(differing, {"path_corroboration_recorded"})

    def test_a_root_that_exists_on_no_filesystem_resolves_identically(self) -> None:
        """A root naming nothing at all still resolves, because nothing is read.

        Stated as its own assertion because it is the property the whole hermetic design
        rests on: the expected counters were derived under a root this test process may not
        have, and that is sound precisely because no path here is opened.
        """
        absent_root = "/nonexistent-root-for-this-assertion/spark-src"
        self.assertFalse(Path(absent_root).exists(), msg="the root must genuinely not exist")
        record = anchored_check(SPARK_DOCKERFILE, absent_root, file_line_range=[36, 48])
        adapted = self.adapt(object_form(record), root=absent_root)
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["path"], SPARK_DOCKERFILE)
        self.assertTrue(adapted.rows[0]["in_scope"])


# ======================================================================================
# 2. The leading-slash rule -- the user's worked example (AAP 0.5.3; brief assertions 6-7)
# ======================================================================================


class LeadingSlashTests(CheckovAdapterTestCase):
    """Checkov's slash-carrying paths, and the silent regression the example warns about.

    The example, preserved as given: ``file_path`` is relative to the scan root **and
    carries a leading slash**, as in ``/folder1/A.tf``. **Reading that slash as
    filesystem-absolute produces a long ``../`` path and a false ``in_scope: false``.**
    Nothing raises, so the assertions here take both halves -- first that the mis-reading
    really would produce the chain, then that the emitted value does not.
    """

    def test_a_real_in_scope_dockerfile_emits_the_root_relative_form(self) -> None:
        """``/resource-managers/.../spark/Dockerfile`` -> the same path without the slash.

        A real path, verified present at the pin alongside its ``bindings/python`` and
        ``bindings/R`` siblings, rather than an invented ``.tf`` file: the Kubernetes docker
        tree is the realistic in-scope Checkov surface, and it is scanned by the file-based
        tools while being no Maven module at all.
        """
        root = recorded_env().root
        for relative in (SPARK_DOCKERFILE, PYTHON_DOCKERFILE, R_DOCKERFILE):
            with self.subTest(path=relative):
                adapted = self.adapt(object_form(anchored_check(relative, root)))
                self.assertEqual(len(adapted.rows), 1, msg="one record, one row")
                row = adapted.rows[0]
                self.assertEqual(row["path"], relative)
                self.assertTrue(row["in_scope"], msg="an in-scope Dockerfile is in scope")
                self.assertRelativeInsideTree(row["path"], context=relative)

    def test_the_mis_reading_the_example_warns_about_really_produces_a_parent_chain(self) -> None:
        """The trap is real: read as filesystem-absolute, the value relativizes to ``../``.

        Measured through ``paths.relativize_to_root`` -- the operation an implementation
        would reach for if it read the leading slash as absoluteness -- so the regression is
        demonstrated rather than described. Without this half, the assertion that the emitted
        path carries no ``../`` would be consistent with there being no trap at all.
        """
        root = recorded_env().root
        slash_carrying = f"/{SPARK_DOCKERFILE}"
        mis_read = paths.relativize_to_root(slash_carrying, root)
        self.assertIn(
            "../",
            mis_read,
            msg=(
                "reading repo_file_path as filesystem-absolute must produce the parent "
                f"chain the worked example predicts; observed {mis_read!r}"
            ),
        )
        self.assertEqual(
            paths.path_kind_for(mis_read),
            paths.PATH_KIND_OUTSIDE_ROOT,
            msg="and the mis-read value is an outside-root coordinate",
        )
        self.assertFalse(
            paths.in_scope(mis_read, recorded_env().globs, kind=paths.PATH_KIND_OUTSIDE_ROOT),
            msg="which is exactly the false in_scope: false the example warns about",
        )

    def test_the_emitted_path_carries_no_parent_segment_and_is_never_absolute(self) -> None:
        """Every row of every fixture: relative, inside the tree, no ``../``, not a URI.

        Asserted across all seven fixtures rather than on one record, because the regression
        is uniform when it happens -- a whole tool's rows go wrong in the same direction,
        which is far harder to notice than an error.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for index, row in enumerate(adapted.rows):
                    self.assertRelativeInsideTree(
                        row["path"], context=f"{stem} row {index}"
                    )
                self.assertSchemaClean(adapted.rows, context=stem)

    def test_a_hostile_target_relative_file_path_does_not_reach_the_row(self) -> None:
        """The deliberately hostile case: a short ``file_path`` under a deep anchor.

        The runner passes one ``-d`` per scope directory, so a record's ``file_path`` is
        relative to whichever target matched -- here ``/dockerfiles/spark/Dockerfile`` beside
        an anchor naming the full root-relative path. Two failures are possible and both are
        ruled out: reading ``file_path`` as absolute yields a ``../`` chain, and stripping its
        slash and joining it to the root names a directory that does not exist. The anchored
        path is what reaches the row.
        """
        root = recorded_env().root
        hostile_file_path = "/dockerfiles/spark/Dockerfile"
        as_absolute = paths.relativize_to_root(hostile_file_path, root)
        self.assertIn("../", as_absolute, msg="the absolute mis-reading yields a chain")
        stripped_and_joined = paths.strip_single_leading_slash(hostile_file_path)
        self.assertNotEqual(
            stripped_and_joined,
            SPARK_DOCKERFILE,
            msg=(
                "and the strip-and-join mis-reading names a different path entirely, even "
                "with the slash handled correctly"
            ),
        )

        adapted = self.adapt(
            object_form(
                anchored_check(
                    SPARK_DOCKERFILE, root, file_path=hostile_file_path, file_line_range=[36, 48]
                )
            )
        )
        self.assertEqual(len(adapted.rows), 1)
        row = adapted.rows[0]
        self.assertEqual(row["path"], SPARK_DOCKERFILE)
        self.assertRelativeInsideTree(row["path"], context="hostile file_path")
        self.assertTrue(row["in_scope"], msg="and the row stays in scope")
        self.assertEqual(
            adapted.counters["path_corroboration_recorded"],
            0,
            msg="a target-relative file_path that is a suffix of the anchor corroborates it",
        )

    def test_exactly_one_leading_slash_is_removed(self) -> None:
        """One slash, not every slash -- and a ``//`` prefix stays visible as an authority.

        ``paths.strip_single_leading_slash`` is the operation the rule names, and a ``//``
        prefix is an authority rather than a path: flattening it quietly would turn a value
        that must be rejected into one that looks relative.
        """
        self.assertEqual(
            paths.strip_single_leading_slash(f"/{SPARK_DOCKERFILE}"), SPARK_DOCKERFILE
        )
        self.assertEqual(
            paths.strip_single_leading_slash(SPARK_DOCKERFILE),
            SPARK_DOCKERFILE,
            msg="a value without a leading slash is returned unchanged",
        )
        self.assertTrue(
            paths.strip_single_leading_slash("//authority/path").startswith("//"),
            msg="a // prefix is preserved so it can be refused rather than flattened",
        )


class AnchorReconciliationTests(CheckovAdapterTestCase):
    """``file_abs_path`` reconciliation -- brief assertions 8 and 9.

    The recorded invocation is what makes this the *reliable* resolution route rather than a
    cross-check: with several ``-d`` roots in one invocation, a slash-stripped ``file_path``
    is relative to whichever root matched, so an anchor field is the only thing that says
    which target a record came from.

    AAP 0.5.3 and AAP 0.1.3 pull in two different directions and both are honoured, because
    they describe two different situations: a disagreement that *can* be resolved is
    **recorded** and the row is kept, while a record with no resolvable anchor is
    **rejected and counted**. Each is asserted separately below.
    """

    def test_a_consistent_file_abs_path_agrees_with_the_resolution(self) -> None:
        """Where both anchors are present and consistent, the resolution matches either.

        The equality is computed rather than assumed: ``file_abs_path`` is relativized to the
        root through ``paths.relativize_to_root`` and compared with the emitted path, so the
        agreement is measured on the same value the resolver saw.
        """
        root = recorded_env().root
        record = anchored_check(PYTHON_DOCKERFILE, root, file_line_range=[32, 36])
        adapted = self.adapt(object_form(record))
        self.assertEqual(len(adapted.rows), 1)
        row = adapted.rows[0]
        self.assertEqual(row["path"], PYTHON_DOCKERFILE)
        self.assertEqual(
            row["path"],
            paths.relativize_to_root(record["file_abs_path"], root),
            msg="the relativized file_abs_path is the same path the anchor produced",
        )
        self.assertEqual(
            row["path"],
            paths.strip_single_leading_slash(record["repo_file_path"]),
            msg="and so is the slash-stripped repo_file_path",
        )
        self.assertEqual(adapted.counters["path_corroboration_recorded"], 0)

    def test_a_resolvable_disagreement_is_recorded_and_keeps_the_row(self) -> None:
        """Two anchors that both resolve but disagree: recorded, counted, never suppressed.

        AAP 0.5.3 requires a mismatch to be *recorded* rather than silently resolved in favour
        of one field. The row is emitted from the first anchor in the recorded order --
        ``repo_file_path`` -- and ``path_corroboration_recorded`` is what makes the
        disagreement visible instead of a silent preference. It is emphatically not a
        rejection: the record was attributable, and one of the two fields is the recorded
        anchor.
        """
        root = recorded_env().root
        record = anchored_check(
            SPARK_DOCKERFILE, root, file_abs_path=f"{root}/{R_DOCKERFILE}"
        )
        adapted = self.adapt(object_form(record))
        self.assertEqual(len(adapted.rows), 1, msg="the row is kept")
        self.assertEqual(adapted.rejections, [], msg="and it is not a rejection")
        self.assertEqual(
            adapted.rows[0]["path"],
            SPARK_DOCKERFILE,
            msg="the first anchor in the recorded order supplies the path",
        )
        self.assertEqual(
            adapted.counters["path_corroboration_recorded"],
            1,
            msg="and the disagreement is counted rather than dropped",
        )
        resolved = paths.resolve_checkov_path(record, root, recorded_env().tool_base)
        self.assertIsInstance(resolved, paths.ResolvedPath)
        self.assertIsNotNone(
            resolved.corroboration,
            msg="the resolver records the disagreement in words, for the record",
        )
        self.assertIn("file_abs_path", resolved.corroboration)
        self.assertEqual(resolved.basis, paths.BASIS_CHECKOV_REPO_FILE_PATH)

    def test_an_unreconcilable_record_is_rejected_under_a_named_class(self) -> None:
        """No resolvable anchor: rejected and counted, never resolved from one candidate.

        Under ``per_target_directory`` a record carrying only ``file_path`` cannot be placed
        -- there is no way to know which target it came from, and the scan root is not a
        substitute for the answer. AAP 0.1.3: rejected rather than guessed into a field. The
        class is asserted by name against ``paths.REJECT_UNRESOLVABLE_PATH``.
        """
        root = recorded_env().root
        record = anchored_check(
            SPARK_DOCKERFILE,
            root,
            file_path="/dockerfiles/spark/Dockerfile",
            repo_file_path=_OMIT,
            file_abs_path=_OMIT,
        )
        adapted = self.adapt(object_form(record))
        self.assertEqual(adapted.rows, [], msg="no row is emitted from a guess")
        self.assertEqual(len(adapted.rejections), 1, msg="exactly one rejection")
        rejection = adapted.rejections[0]
        self.assertEqual(rejection.reject_class, paths.REJECT_UNRESOLVABLE_PATH)
        self.assertIn(rejection.reject_class, paths.REJECT_CLASSES)
        self.assertIn(
            "file_path",
            rejection.detail,
            msg="the diagnostic names the field it could not place",
        )
        self.assertEqual(rejection.tool, checkov.TOOL)
        self.assertEqual(
            adapted.raw_records,
            len(adapted.rows) + len(adapted.rejections),
            msg="the rejected record still counts on both sides of the identity",
        )

    def test_a_record_naming_no_location_at_all_is_the_absent_path_class(self) -> None:
        """``absent_path`` -- the sixth class, reached by a record with no path field.

        ``path`` is not an optional field (AAP 0.5.4), so a record naming no location is
        rejected and counted rather than emitted with a null path. This is the class the five
        committed negative fixtures do not carry, and it is asserted here so that every class
        this adapter can produce has an assertion.
        """
        record = failed_check(
            check_id=SYNTHETIC_CHECK_ID,
            check_name=SYNTHETIC_CHECK_NAME,
            file_line_range=[1, 1],
        )
        adapted = self.adapt(object_form(record))
        self.assertEqual(adapted.rows, [])
        self.assertEqual(len(adapted.rejections), 1)
        rejection = adapted.rejections[0]
        self.assertEqual(rejection.reject_class, paths.REJECT_ABSENT_PATH)
        self.assertIn(rejection.reject_class, PRODUCIBLE_REJECT_CLASSES)
        self.assertEqual(dict(rejection.record_identity), {"check_id": SYNTHETIC_CHECK_ID})

    def test_repo_file_path_alone_resolves_without_file_abs_path(self) -> None:
        """``file_abs_path`` absent, ``repo_file_path`` present: the anchor still resolves.

        The recorded anchor order is ``[repo_file_path, file_abs_path]``, so the second anchor
        being absent changes nothing -- and the corroboration note says only that there was
        nothing to compare it with, which never suppresses the row.
        """
        root = recorded_env().root
        record = anchored_check(R_DOCKERFILE, root, file_abs_path=_OMIT)
        adapted = self.adapt(object_form(record))
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["path"], R_DOCKERFILE)
        self.assertEqual(adapted.rejections, [])
        resolved = paths.resolve_checkov_path(record, root, recorded_env().tool_base)
        self.assertEqual(resolved.basis, paths.BASIS_CHECKOV_REPO_FILE_PATH)

    def test_file_abs_path_alone_resolves_by_relativization(self) -> None:
        """``repo_file_path`` absent: the absolute anchor is relativized to the root.

        The branch is chosen by *which field it is*, never by whether the value looks
        absolute -- which is the whole of the worked example. ``file_abs_path`` is absolute by
        contract and is relativized; ``repo_file_path`` is root-relative by contract and has
        exactly one slash stripped.
        """
        root = recorded_env().root
        record = anchored_check(PYTHON_DOCKERFILE, root, repo_file_path=_OMIT)
        adapted = self.adapt(object_form(record))
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["path"], PYTHON_DOCKERFILE)
        resolved = paths.resolve_checkov_path(record, root, recorded_env().tool_base)
        self.assertEqual(resolved.basis, paths.BASIS_CHECKOV_FILE_ABS_PATH)

    def test_the_file_path_fallback_applies_only_where_one_target_is_the_scan_root(self) -> None:
        """The documented fallback branch, exercised in the only configuration that has it.

        Where the recorded ``path_base.kind`` is ``scan_root`` -- a single target equal to the
        scan root -- a record carrying nothing but ``file_path`` really is root-relative once
        its leading slash is stripped, and saying so is not a guess. The same record under
        ``per_target_directory`` is the rejection asserted above, which is what makes this a
        property of the recorded configuration rather than of the record.
        """
        environment = single_target_env()
        record = failed_check(
            check_id=SYNTHETIC_CHECK_ID,
            check_name=SYNTHETIC_CHECK_NAME,
            file_path=f"/{SPARK_DOCKERFILE}",
            file_line_range=[36, 48],
        )
        adapted = self.adapt(object_form(record), env=environment)
        self.assertEqual(len(adapted.rows), 1, msg="the fallback resolves rather than rejects")
        self.assertEqual(adapted.rows[0]["path"], SPARK_DOCKERFILE)
        self.assertTrue(adapted.rows[0]["in_scope"])
        resolved = paths.resolve_checkov_path(record, environment.root, environment.tool_base)
        self.assertEqual(resolved.basis, paths.BASIS_CHECKOV_FILE_PATH)
        self.assertIsNotNone(
            resolved.corroboration,
            msg="and the resolver states which route it took",
        )

        under_per_target = self.adapt(object_form(record), env=recorded_env())
        self.assertEqual(
            [rejection.reject_class for rejection in under_per_target.rejections],
            [paths.REJECT_UNRESOLVABLE_PATH],
            msg="the identical record is unresolvable under the recorded configuration",
        )


# ======================================================================================
# 3. The in_scope field -- brief assertion 10
# ======================================================================================


class ScopeFieldTests(CheckovAdapterTestCase):
    """``in_scope`` is decided by the allowlist alone, and nothing is ever dropped.

    A row from a directory the runner reached but the allowlist does not cover is kept with
    ``in_scope: false`` and counted (AAP 0.9.3, AAP 0.6.4). The runner's reach is shaped by
    its own targets; the field is the allowlist's answer, and the two are different
    questions.
    """

    def test_a_dockerfile_matches_through_the_mid_path_recursive_segment(self) -> None:
        """The case a naive matcher drops: ``**`` absorbing an interior segment.

        ``resource-managers/kubernetes/**/src/main/**`` is one of the two globs that
        multiply, and a Dockerfile under ``docker/src/main`` matches only if the mid-path
        ``**`` absorbs the single segment ``docker``. Python's ``fnmatch`` and
        ``PurePath.match`` do not provide zero-or-more-directories semantics, so AAP 0.5.4
        requires the matcher to implement them -- and getting it wrong drops whole modules
        silently, which looks exactly like a module with nothing to report.
        """
        globs = recorded_env().globs
        for relative, absorbed in (
            (SPARK_DOCKERFILE, "docker"),
            (PYTHON_DOCKERFILE, "docker"),
            (VOLCANO_FEATURE_STEP, "core/volcano"),
        ):
            with self.subTest(path=relative):
                decision = paths.scope_decision(relative, globs)
                self.assertTrue(decision.in_scope, msg=decision.reason())
                self.assertEqual(
                    decision.matched_glob,
                    KUBERNETES_GLOB,
                    msg=f"{relative} must match through the mid-path ** absorbing {absorbed}",
                )
                self.assertEqual(decision.kind, paths.PATH_KIND_TREE_FILE)

    def test_every_row_of_the_positive_fixtures_matches_the_recorded_glob(self) -> None:
        """Each expected row's recorded ``matched_allowlist_glob`` is the glob it matches.

        The expectation records which glob each row matched, so the match is checked against
        a hand-verified value rather than against whatever the matcher happens to return.
        """
        globs = recorded_env().globs
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)
                adapted = self.adapt_fixture(stem)
                derivations = expected["row_derivations"]
                self.assertEqual(len(derivations), len(adapted.rows))
                for index, (row, derivation) in enumerate(zip(adapted.rows, derivations)):
                    decision = paths.scope_decision(row["path"], globs)
                    self.assertEqual(
                        decision.matched_glob,
                        derivation["matched_allowlist_glob"],
                        msg=f"{stem} row {index}: matched glob",
                    )
                    self.assertEqual(
                        row["in_scope"],
                        decision.in_scope,
                        msg=f"{stem} row {index}: in_scope follows the allowlist alone",
                    )

    def test_a_real_path_outside_the_twelve_globs_is_kept_out_of_scope(self) -> None:
        """The pin's three lockfiles: kept, counted, and ``in_scope: false``.

        Real files inside the root that no glob covers. A dependency-oriented runner reaches
        them legitimately, and nothing here filters them: AAP 0.9.3 has such a row kept with
        ``in_scope: false``, and the ``rows_out_of_scope`` counter is what makes the number
        visible.
        """
        root = recorded_env().root
        records = [anchored_check(relative, root) for relative in OUT_OF_SCOPE_FILES]
        adapted = self.adapt(object_form(*records))
        self.assertEqual(len(adapted.rows), len(OUT_OF_SCOPE_FILES), msg="every row is kept")
        self.assertEqual(adapted.rejections, [], msg="out of scope is not a rejection")
        self.assertEqual(adapted.paths_emitted, list(OUT_OF_SCOPE_FILES))
        for row in adapted.rows:
            self.assertFalse(row["in_scope"], msg=f"{row['path']} matches no glob")
        self.assertEqual(adapted.counters["rows_out_of_scope"], len(OUT_OF_SCOPE_FILES))
        self.assertEqual(adapted.counters["rows_in_scope"], 0)
        self.assertEqual(
            adapted.counters["non_filesystem_paths"],
            0,
            msg="a file inside the root is a tree_file coordinate, out of scope or not",
        )

    def test_a_python_test_module_is_in_scope(self) -> None:
        """``python/pyspark/ml/tests/test_evaluation.py`` is in scope, and must stay so.

        It carries **no** ``src/test`` segment and sits inside the authoritative glob
        ``python/pyspark/**``. AAP 0.3.1 is explicit: the exclusion is the literal
        ``src/test``, which removes every Scala and Java test tree and removes nothing from
        ``python/pyspark/**`` -- whose 832 test modules are part of the 4,095-file expansion
        and are scanned like any other in-scope source. The loose reading of "tests are out
        of scope" would drop a fifth of the in-scope file count, so it must not flip this row.
        """
        root = recorded_env().root
        adapted = self.adapt(
            object_form(anchored_check(PYSPARK_TEST_MODULE, root, file_line_range=[17, 17]))
        )
        self.assertEqual(len(adapted.rows), 1)
        row = adapted.rows[0]
        self.assertEqual(row["path"], PYSPARK_TEST_MODULE)
        self.assertTrue(row["in_scope"], msg="a python/pyspark test module is in scope")
        decision = paths.scope_decision(row["path"], recorded_env().globs)
        self.assertEqual(decision.matched_glob, "python/pyspark/**")
        self.assertFalse(decision.excluded_by_src_test)
        self.assertNotIn(paths.SRC_TEST_MARKER, row["path"])

    def test_a_scala_test_source_is_out_of_scope_by_the_literal_marker(self) -> None:
        """A real ``src/test`` path is out of scope, and for the stated reason.

        The reason is asserted as well as the verdict: ``excluded_by_src_test`` is what says
        the row is out of scope because of the literal marker rather than because it happened
        to match no glob -- and the exclusion overrides a positive glob match.
        """
        root = recorded_env().root
        adapted = self.adapt(object_form(anchored_check(SCALA_TEST_SOURCE, root)))
        self.assertEqual(len(adapted.rows), 1, msg="the row is kept, not dropped")
        row = adapted.rows[0]
        self.assertEqual(row["path"], SCALA_TEST_SOURCE)
        self.assertFalse(row["in_scope"])
        decision = paths.scope_decision(row["path"], recorded_env().globs)
        self.assertTrue(decision.excluded_by_src_test, msg=decision.reason())
        self.assertIn(paths.SRC_TEST_MARKER, row["path"])

    def test_the_in_scope_decomposition_sums_to_the_row_count(self) -> None:
        """``rows_in_scope + rows_out_of_scope`` is the row count, on every fixture.

        One measurement split rather than a second count of the same thing, so a row that
        reached neither bucket would be visible here.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertEqual(
                    adapted.counters["rows_in_scope"] + adapted.counters["rows_out_of_scope"],
                    len(adapted.rows),
                )


# ======================================================================================
# 4. Failures only, and parsing errors as status evidence (brief assertions 11-13)
# ======================================================================================


class FailuresOnlyTests(CheckovAdapterTestCase):
    """``passed_checks`` and ``skipped_checks`` produce no rows -- brief assertion 11.

    Asserted from a fixture that actually contains both. That fixture is the **derived
    feature fixture**, not the capture, and the reason is a property of this run's artifact:
    the runner invokes checkov with ``--compact``, so ``harness/artifacts/raw/checkov.json``
    carries ``results.failed_checks`` alone -- no ``passed_checks`` key, no
    ``skipped_checks`` key, no ``parsing_errors`` key. An exclusion asserted over that
    document would be vacuous: there is nothing in it to exclude. The capture is still
    asserted here, for the other half of the contract -- an absent bucket must be read as
    zero rather than raising -- while the exclusion itself is asserted against
    ``derived-checkov-features.json``, which carries three passed checks, one skipped check
    and one parsing error beside its five failures.

    AAP 0.5.4 states the rule -- only failures are findings -- and AAP 0.2.2 records why:
    Checkov's ``results`` object carries all three buckets when it is not run compact, so
    the adapter counts and emits failed checks only.
    """

    def test_the_feature_fixture_really_carries_passed_and_skipped_checks(self) -> None:
        """State what the input contains before asserting what was done with it.

        An exclusion asserted over a fixture with nothing to exclude would establish nothing.
        The numbers come from the fixture's own hand-verified expectation, so this test and
        the expectation cannot drift apart while both still pass.
        """
        recorded = load_expected(FEATURE_FIXTURE)["fixture"]
        buckets = bucket_counts(load_fixture(FEATURE_FIXTURE))
        self.assertEqual(buckets[EMITTED_SECTION], recorded[EMITTED_SECTION])
        self.assertEqual(buckets["passed_checks"], recorded["passed_checks"])
        self.assertEqual(buckets["skipped_checks"], recorded["skipped_checks"])
        self.assertEqual(buckets[PARSING_ERRORS_SECTION], recorded[PARSING_ERRORS_SECTION])
        self.assertGreater(buckets["passed_checks"], 0, msg="passes must be present")
        self.assertGreater(buckets["skipped_checks"], 0, msg="skips must be present")
        self.assertGreater(buckets[PARSING_ERRORS_SECTION], 0, msg="a parse error too")

    def test_the_captured_artifact_omits_the_buckets_and_they_read_as_zero(self) -> None:
        """The artifact's own shape: one bucket present, three keys absent entirely.

        This is the case the capture is the only honest source for, and it is a real branch
        rather than a formality -- ``results.get(bucket)`` returning ``None`` has to be read
        as zero observed records, not as a malformed report. Asserted on the capture and on
        its array-form counterpart, both of which carry ``failed_checks`` alone.
        """
        for stem in SHAPE_PAIR:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                reports = document if isinstance(document, list) else [document]
                for index, report_object in enumerate(reports):
                    with self.subTest(report=index):
                        self.assertEqual(
                            set(report_object["results"]),
                            {EMITTED_SECTION},
                            msg=f"{stem}: report {index} carries one bucket",
                        )
                buckets = bucket_counts(document)
                self.assertGreater(buckets[EMITTED_SECTION], 0)
                for bucket in (*NEVER_EMITTED_SECTIONS, PARSING_ERRORS_SECTION):
                    self.assertEqual(
                        buckets[bucket],
                        0,
                        msg=f"{stem}: an absent {bucket} key reads as zero",
                    )
                adapted = self.adapt(document)
                self.assertEqual(adapted.counters["passed_checks_observed"], 0)
                self.assertEqual(adapted.counters["skipped_checks_observed"], 0)
                self.assertEqual(adapted.counters[PARSING_ERRORS_SECTION], 0)
                self.assertEqual(adapted.rejections, [], msg=f"{stem}: and nothing is rejected")

    def test_the_row_count_is_exactly_the_failed_check_count_of_every_positive_fixture(
        self,
    ) -> None:
        """One row per failed check -- and no pass, skip or parse error leaks into any.

        The exactness is the assertion. On the feature fixture any one of the three passed
        checks, the skipped check or the parsing error leaking in would make the count six,
        seven, eight or nine instead of five; on the capture and its array counterpart the
        count is the artifact's six failed checks. Both numbers come from the independent
        traversal and from each fixture's own hand-verified expectation rather than from a
        literal restated here.
        """
        for stem in POSITIVE_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                adapted = self.adapt(document)
                failed = failed_check_union(document)
                expected = load_expected(stem)
                self.assertEqual(
                    len(adapted.rows),
                    len(failed),
                    msg=f"{stem}: one row per failed check and nothing else",
                )
                self.assertEqual(
                    len(adapted.rows),
                    len(expected["rows"]),
                    msg=f"{stem}: and exactly the rows its expectation hand-derived",
                )
                self.assertEqual(
                    len(failed),
                    expected["counts"]["raw_finding_records"],
                    msg=f"{stem}: the count unit its expectation was derived against",
                )

    def test_no_row_corresponds_to_a_passed_or_skipped_check(self) -> None:
        """Checked by locator triple, not by check identifier.

        By identifier alone this would be unsound: ``CKV_DOCKER_9`` appears both as a failure
        on two Dockerfiles and as the skipped check on a third. The bucket decides the
        outcome, never the identifier -- so the comparison is on the
        ``(check_id, path, start_line)`` triple each record names.

        Run over every positive fixture, and guarded: the capture and its array counterpart
        carry no never-emitted record at all, so the loop over them is empty by construction
        and the count of records actually examined is asserted afterwards. Without that
        guard this test would keep passing if the feature fixture ever lost its buckets.
        """
        examined = 0
        for stem in POSITIVE_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                adapted = self.adapt(document)
                emitted = {
                    (row["rule_id"], row["path"], row["start_line"]) for row in adapted.rows
                }
                for bucket in NEVER_EMITTED_SECTIONS:
                    for record in bucket_records(document, bucket):
                        examined += 1
                        locator = (
                            record["check_id"],
                            paths.strip_single_leading_slash(record["repo_file_path"]),
                            record["file_line_range"][0],
                        )
                        self.assertNotIn(
                            locator,
                            emitted,
                            msg=f"{stem}: a {bucket} record reached the rows as {locator}",
                        )
        recorded = load_expected(FEATURE_FIXTURE)["fixture"]
        self.assertEqual(
            examined,
            recorded["passed_checks"] + recorded["skipped_checks"],
            msg=(
                "the exclusion must be measured against real never-emitted records; "
                f"{examined} were examined across the positive fixtures"
            ),
        )
        self.assertGreater(examined, 0, msg="an exclusion with nothing to exclude proves nothing")

    def test_the_skipped_check_shares_its_identifier_with_two_emitted_failures(self) -> None:
        """The bucket decides, never the identifier -- stated as its own assertion.

        ``CKV_DOCKER_9`` is skipped on ``bindings/R/Dockerfile`` and fails on two other
        Dockerfiles. Both facts must hold at once: the identifier appears in the rows, and the
        skipped record's own location does not.

        Asserted against the feature fixture, which is where the skipped bucket lives. The
        record is not an invention: it is the artifact's own sixth failed check --
        ``CKV_DOCKER_9`` on ``bindings/R/Dockerfile``, ``file_line_range`` ``[34, 37]`` --
        relocated into ``skipped_checks``, which is exactly why the identifier collision it
        creates is a real one.
        """
        document = load_fixture(FEATURE_FIXTURE)
        adapted = self.adapt(document)
        skipped = bucket_records(document, "skipped_checks")[0]
        self.assertEqual(skipped["check_id"], "CKV_DOCKER_9")
        self.assertIn(
            "CKV_DOCKER_9",
            [row["rule_id"] for row in adapted.rows],
            msg="the identifier does appear, on records that failed",
        )
        skipped_path = paths.strip_single_leading_slash(skipped["repo_file_path"])
        self.assertNotIn(
            (skipped["check_id"], skipped_path, skipped["file_line_range"][0]),
            {(row["rule_id"], row["path"], row["start_line"]) for row in adapted.rows},
            msg="but the skipped record's own location does not",
        )

    def test_the_never_emitted_buckets_are_counted_as_status_evidence(self) -> None:
        """Counted so that "produced no row" is an observation rather than a claim.

        Neither counter feeds reconciliation, and neither can produce a row. Their value is
        that a reader of ``tool-status.md`` can see the passes were present and produced
        nothing -- which requires the counter to be non-zero somewhere, or the channel is
        only asserted at rest. The feature fixture is where it is non-zero, and the numbers
        are the ones its own expectation hand-derived.
        """
        recorded = load_expected(FEATURE_FIXTURE)
        adapted = self.adapt_fixture(FEATURE_FIXTURE)
        self.assertEqual(
            adapted.counters["passed_checks_observed"],
            recorded["fixture"]["passed_checks"],
        )
        self.assertEqual(
            adapted.counters["skipped_checks_observed"],
            recorded["fixture"]["skipped_checks"],
        )
        self.assertEqual(
            adapted.counters["passed_checks_observed"],
            recorded["counters"]["passed_checks_observed"],
        )
        self.assertEqual(
            adapted.counters["skipped_checks_observed"],
            recorded["counters"]["skipped_checks_observed"],
        )
        self.assertGreater(adapted.counters["passed_checks_observed"], 0)
        self.assertGreater(adapted.counters["skipped_checks_observed"], 0)
        self.assertEqual(
            len(adapted.rows),
            len(recorded["rows"]),
            msg="and the rows are unaffected by either bucket",
        )

    def test_a_document_of_only_passes_and_skips_yields_nothing(self) -> None:
        """The degenerate case, in both shapes: no failures, no rows, no rejections.

        Built from named records rather than a fixture, because no committed artifact has this
        shape -- and it is the cleanest statement of the contract: the two buckets are read
        only to be counted.
        """
        root = recorded_env().root
        passed = [anchored_check(SPARK_DOCKERFILE, root, check_id="CKV_DOCKER_3")]
        skipped = [anchored_check(R_DOCKERFILE, root, check_id="CKV_DOCKER_9")]
        documents = {
            "object form": object_form(passed=passed, skipped=skipped),
            "array form": array_form(report(passed=passed, skipped=skipped)),
        }
        for label, document in documents.items():
            with self.subTest(document=label):
                adapted = self.adapt(document)
                self.assertEqual(adapted.rows, [], msg=f"{label}: no rows")
                self.assertEqual(adapted.rejections, [], msg=f"{label}: no rejections")
                self.assertEqual(adapted.raw_records, 0)
                self.assertEqual(adapted.counters["passed_checks_observed"], 1)
                self.assertEqual(adapted.counters["skipped_checks_observed"], 1)


class ParsingErrorsTests(CheckovAdapterTestCase):
    """``parsing_errors`` are status evidence, not findings -- brief assertion 12.

    A rejection describes a *record* the adapter could not attribute; a parsing error is
    Checkov's own report about a file it could not read, and it is not one of the counted
    failed checks. Converting one into a rejection would put it on the right-hand side of
    ``raw finding records = dataset rows + rejected records`` while the independent traversal
    never put it on the left, breaking the identity for something that was never a finding.
    """

    def test_a_parsing_error_produces_neither_a_row_nor_a_rejection(self) -> None:
        """A real parse error present; zero rows and zero rejections from it.

        Asserted on the feature fixture, because the captured artifact was written compact
        and carries no ``parsing_errors`` key at all -- a document with no parse error cannot
        establish that a parse error is excluded. The row and rejection counts are exactly
        the failed-check split, so the parse error contributed to neither, and the
        independent traversal never counted it either.
        """
        document = load_fixture(FEATURE_FIXTURE)
        adapted = self.adapt(document)
        expected = load_expected(FEATURE_FIXTURE)
        parse_errors = bucket_counts(document)[PARSING_ERRORS_SECTION]
        self.assertGreater(parse_errors, 0, msg="the fixture must carry a parse error")
        self.assertEqual(parse_errors, expected["fixture"][PARSING_ERRORS_SECTION])
        self.assertEqual(len(adapted.rows), len(expected["rows"]))
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(adapted.raw_records, expected["counts"]["raw_finding_records"])
        self.assertEqual(adapted.counters[PARSING_ERRORS_SECTION], parse_errors)

    def test_the_captured_artifact_carries_no_parsing_errors_and_counts_none(self) -> None:
        """The other half: an absent ``parsing_errors`` key is zero, not an error.

        The runner's ``--compact`` invocation omits the key, so this is the shape the real
        artifact has and the branch a reader most needs pinned -- ``results.get`` returning
        ``None`` must read as zero entries rather than raising or being skipped past.
        """
        for stem in SHAPE_PAIR:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                adapted = self.adapt(document)
                self.assertEqual(bucket_counts(document)[PARSING_ERRORS_SECTION], 0)
                self.assertEqual(list(checkov.collect_parsing_errors(document)), [])
                self.assertEqual(adapted.counters[PARSING_ERRORS_SECTION], 0)
                self.assertEqual(
                    load_expected(stem)["failures_only"]["parsing_errors_entries"],
                    [],
                    msg=f"{stem}: and its expectation records the absence",
                )

    def test_the_entries_are_surfaced_verbatim_for_tool_status(self) -> None:
        """``collect_parsing_errors`` returns the document's own entries, unrendered.

        ``tool-status.md``'s per-tool contract requires *any parser error verbatim*, so the
        entry is returned exactly as the document carries it -- with the report index and
        ``check_type`` beside it so a reader can find it again. Rendering or truncating it
        would make it evidence a reader cannot see in the producer's own words.

        Run over every positive fixture and then guarded on the total surfaced, because two
        of the three legitimately carry none: without the guard this assertion would go
        vacuous the moment the fixture that does carry one stopped doing so.
        """
        surfaced_total = 0
        for stem in POSITIVE_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                surfaced = checkov.collect_parsing_errors(document)
                recorded = load_expected(stem)["failures_only"]["parsing_errors_entries"]
                self.assertEqual([entry["entry"] for entry in surfaced], recorded)
                for entry in surfaced:
                    surfaced_total += 1
                    self.assertIn("report_index", entry)
                    self.assertIn("check_type", entry)
                    self.assertEqual(entry["check_type"], "dockerfile")
        self.assertEqual(
            surfaced_total,
            load_expected(FEATURE_FIXTURE)["fixture"][PARSING_ERRORS_SECTION],
            msg="at least one entry must really have been surfaced and inspected",
        )
        self.assertGreater(surfaced_total, 0)

    def test_the_parsing_error_count_and_the_verbatim_list_cannot_disagree(self) -> None:
        """The counter is taken through the same public traversal the entries come from.

        So the number in ``normalize-run.json`` and the list in ``tool-status.md`` are one
        measurement cited twice, which is what AAP 0.6.4 requires of a count appearing in two
        documents.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                adapted = self.adapt(document)
                self.assertEqual(
                    adapted.counters[PARSING_ERRORS_SECTION],
                    len(checkov.collect_parsing_errors(document)),
                )

    def test_report_summaries_return_the_tools_own_words_beside_them(self) -> None:
        """``report_summaries`` carries each report's ``check_type`` and ``summary`` verbatim.

        The tool's statement about itself -- ``passed``/``failed``/``skipped``/
        ``parsing_errors``/``resource_count`` -- available for ``tool-status.md`` without this
        module ever publishing a substitute for the record count. The summary's own ``failed``
        is *not* the count unit, and it is not used as one anywhere here.
        """
        for stem in POSITIVE_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                summaries = checkov.report_summaries(document)
                reports = document if isinstance(document, list) else [document]
                self.assertEqual(len(summaries), len(reports))
                for index, summary in enumerate(summaries):
                    self.assertEqual(summary["report_index"], index)
                    self.assertEqual(summary["check_type"], reports[index]["check_type"])
                    self.assertEqual(summary["summary"], reports[index]["summary"])

    def test_a_non_empty_parsing_errors_list_is_carried_without_becoming_a_finding(self) -> None:
        """A synthetic document whose only content is parse errors: nothing but status.

        Zero rows, zero rejections, the entries available verbatim and the counter equal to
        their number -- the channel exists so a future non-zero count cannot vanish.
        """
        entries = ["entrypoint.sh", "decom.sh"]
        for label, document in (
            ("object form", object_form(parsing_errors=entries)),
            ("array form", array_form(report(parsing_errors=entries))),
        ):
            with self.subTest(document=label):
                adapted = self.adapt(document)
                self.assertEqual(adapted.rows, [])
                self.assertEqual(adapted.rejections, [])
                self.assertEqual(adapted.counters[PARSING_ERRORS_SECTION], len(entries))
                self.assertEqual(
                    [entry["entry"] for entry in checkov.collect_parsing_errors(document)],
                    entries,
                )


class CountUnitUnionTests(CheckovAdapterTestCase):
    """The count unit is the union of ``results.failed_checks[]`` -- brief assertion 13.

    The left-hand side of the reconciliation identity comes from :func:`failed_check_union`,
    a traversal written in this file that resolves no path, maps no severity and builds no
    row. That independence is the point: a count taken from the traversal that builds the
    rows would satisfy the identity while testing nothing. Each result is additionally
    checked against the hand-verified ``counts.raw_finding_records`` in the expected file,
    so two independent authorities have to agree.
    """

    def test_the_identity_holds_on_every_committed_fixture(self) -> None:
        """``raw finding records = dataset rows + rejected records``, on every fixture.

        Non-degenerate on the reject fixtures: over an all-valid document the identity
        collapses to ``raw = rows``, and it is the fixtures in ``REJECT_FIXTURES`` that make
        the right-hand side carry both terms.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertReconciles(adapted, load_expected(stem), context=stem)

    def test_the_union_spans_every_report_object_in_the_array_form(self) -> None:
        """Failed checks in **more than one** element, summed in element order.

        The committed array-form fixture carries its records in a single element, so the union
        semantics across several contributing elements is asserted here on a document built
        from named records: three elements, three different contributions, and the rows in
        element order.
        """
        root = recorded_env().root
        first = [anchored_check(SPARK_DOCKERFILE, root, file_line_range=[36, 48])]
        second = [
            anchored_check(PYTHON_DOCKERFILE, root, check_id="CKV_DOCKER_9"),
            anchored_check(R_DOCKERFILE, root),
        ]
        third = [anchored_check(PYSPARK_TEST_MODULE, root, file_line_range=[17, 17])]
        document = array_form(
            report(*first, check_type="dockerfile"),
            report(check_type="kubernetes"),
            report(*second, check_type="dockerfile"),
            report(*third, check_type="secrets"),
        )
        adapted = self.adapt(document)
        self.assertEqual(adapted.raw_records, 4, msg="the union is the sum, not one element")
        self.assertEqual(len(adapted.rows), 4)
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(
            adapted.paths_emitted,
            [SPARK_DOCKERFILE, PYTHON_DOCKERFILE, R_DOCKERFILE, PYSPARK_TEST_MODULE],
            msg="report objects in order, and within each, failed_checks in order",
        )
        self.assertEqual(adapted.counters["reports"], 4)
        self.assertEqual(
            adapted.counters["reports_without_failed_checks_array"],
            0,
            msg="the empty element carries an empty array, which is still an array",
        )

    def test_an_element_contributing_nothing_does_not_change_the_total(self) -> None:
        """Adding an empty report object changes the counts of containers, not of records."""
        root = recorded_env().root
        records = [anchored_check(SPARK_DOCKERFILE, root)]
        without = self.adapt(array_form(report(*records)))
        with_empty = self.adapt(
            array_form(report(check_type="kubernetes"), report(*records), report(check_type="yaml"))
        )
        self.assertEqual(without.raw_records, with_empty.raw_records)
        self.assertEqual(without.rows, with_empty.rows)
        self.assertEqual(without.counters["reports"], 1)
        self.assertEqual(with_empty.counters["reports"], 3)

    def test_the_scanner_class_never_varies_across_any_record(self) -> None:
        """``misconfig`` on every row of every fixture, and on every synthetic record.

        Fixed by AAP 0.5.4's class table for this tool. ``check_type`` varies across the
        documents exercised here -- ``dockerfile``, ``kubernetes``, ``secrets``, ``yaml`` --
        and the class does not follow it.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for row in adapted.rows:
                    self.assertEqual(row["tool"], checkov.TOOL)
                    self.assertEqual(row["scanner_class"], "misconfig")
                    self.assertEqual(row["scanner_class"], checkov.SCANNER_CLASS)
        root = recorded_env().root
        for check_type in ("dockerfile", "kubernetes", "secrets", "yaml", "terraform"):
            with self.subTest(check_type=check_type):
                adapted = self.adapt(
                    object_form(anchored_check(SPARK_DOCKERFILE, root), check_type=check_type)
                )
                self.assertEqual(adapted.rows[0]["scanner_class"], "misconfig")


# ======================================================================================
# 5. The twelve fields, one by one (brief assertions 14, 15, 17, 18, 19)
# ======================================================================================


class FieldByFieldTests(CheckovAdapterTestCase):
    """Every field of every row, against the expectation and against the record itself.

    Two authorities are used deliberately. The expected file is the hand-verified one, and
    the record is what the artifact actually says -- so a value that agreed with the
    expectation while disagreeing with its own source field would still be caught.
    """

    def test_every_row_of_every_fixture_matches_field_for_field(self) -> None:
        """All seven fixtures, all twelve fields, iterated from ``emit.FIELDS``.

        The field list is the authored constant everything downstream keys on, so a row
        carrying a thirteenth key, missing one, or ordering them differently fails here rather
        than at the CSV writer.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertRowsMatchExpected(adapted, load_expected(stem), context=stem)

    def test_rule_id_and_message_come_from_check_id_and_check_name_verbatim(self) -> None:
        """``rule_id`` <- ``check_id`` and ``message`` <- ``check_name``, unaltered.

        Compared against the record the row came from, located through the expectation's own
        ``check_pointer``, so the source field is the one AAP 0.5.4 names rather than whichever
        field happened to carry a similar value. Nothing is trimmed: a ``check_name`` may
        legitimately carry embedded newlines, which is why output equality is asserted by
        parsing rather than by counting lines elsewhere in this pipeline.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                adapted = self.adapt(document)
                sources = [
                    record
                    for record in failed_check_union(document)
                    if isinstance(record, dict)
                ]
                for row in adapted.rows:
                    matching = [
                        record
                        for record in sources
                        if record.get("check_id") == row["rule_id"]
                        and record.get("check_name") == row["message"]
                    ]
                    self.assertTrue(
                        matching,
                        msg=(
                            f"{stem}: no failed check carries check_id {row['rule_id']!r} "
                            f"with check_name {row['message']!r}"
                        ),
                    )

    def test_bc_check_id_is_never_substituted_for_a_missing_check_id(self) -> None:
        """A record with no ``check_id`` but a ``bc_check_id`` is still rejected.

        AAP 0.5.4 names ``check_id`` as the source of ``rule_id``. Emitting the alternate
        identifier under the same column would make the dataset's ``rule_id`` two things at
        once -- so the alternate is named in the diagnostic and used nowhere else.
        """
        root = recorded_env().root
        record = anchored_check(SPARK_DOCKERFILE, root, check_id=_OMIT)
        record["bc_check_id"] = "BC_DOCKER_2"
        adapted = self.adapt(object_form(record))
        self.assertEqual(adapted.rows, [], msg="no row is emitted from the alternate")
        self.assertEqual(len(adapted.rejections), 1)
        rejection = adapted.rejections[0]
        self.assertEqual(rejection.reject_class, paths.REJECT_MISSING_RULE_ID)
        self.assertIn(
            "bc_check_id",
            rejection.detail,
            msg="the diagnostic says the record has an identifier, just not that one",
        )

    def test_start_line_is_the_integer_at_file_line_range_zero(self) -> None:
        """``start_line`` <- ``file_line_range[0]``, as an ``int`` and never coerced.

        Checked against the expectation's recorded ``file_line_range_in_record`` so the
        comparison is against the value in the artifact rather than against the row's own
        neighbour. Absence is permitted for this field, and the one expected row whose record
        omits the range carries ``None`` -- with ``start_line_absent`` counting it, which is
        the only way that number is visible.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                derivations = load_expected(stem)["row_derivations"]
                absent = 0
                for index, (row, derivation) in enumerate(zip(adapted.rows, derivations)):
                    recorded_range = derivation.get("file_line_range_in_record")
                    if recorded_range is None:
                        self.assertIsNone(
                            row["start_line"],
                            msg=f"{stem} row {index}: an absent range means an absent line",
                        )
                        absent += 1
                        continue
                    self.assertEqual(row["start_line"], recorded_range[0])
                    self.assertIsInstance(row["start_line"], int)
                    self.assertNotIsInstance(
                        row["start_line"], bool, msg="a boolean is not a line number"
                    )
                self.assertEqual(
                    adapted.counters["start_line_absent"],
                    absent,
                    msg=f"{stem}: start_line_absent must equal the rows carrying no line",
                )

    def test_cwe_cve_and_package_coordinate_are_absent_on_every_fixture_row(self) -> None:
        """All three ``None``, and their absence is permitted rather than a rejection.

        A misconfiguration names a location in a configuration file, not a package, so
        ``package_coordinate`` is ``None`` by design -- and AAP 0.5.4 attaches the unformable
        coordinate rejection to a *dependency-oriented* record, which this is not. No record
        in these fixtures carries a weakness identifier in the closed source field set either.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for index, row in enumerate(adapted.rows):
                    self.assertIsNone(row["cwe"], msg=f"{stem} row {index}: cwe")
                    self.assertIsNone(row["cve"], msg=f"{stem} row {index}: cve")
                    self.assertIsNone(
                        row["package_coordinate"],
                        msg=f"{stem} row {index}: package_coordinate is absent by design",
                    )
                self.assertEqual(adapted.counters["multi_valued_cwe_records"], 0)
                self.assertEqual(adapted.counters["multi_valued_cve_records"], 0)

    def test_an_identifier_bearing_record_takes_the_lowest_numbered_one(self) -> None:
        """Where a record does carry identifiers, one value each, by ascending number.

        The ordering is total -- the integer after ``CWE-``, and year then sequence for a CVE
        -- so no tie can arise and no producer-order tiebreak is needed. The identifiers are
        listed out of numeric order in the source field precisely so the selection is
        demonstrated rather than coincidental, and the multi-valued counters record that more
        than one was present.
        """
        root = recorded_env().root
        record = anchored_check(
            SPARK_DOCKERFILE,
            root,
            guideline="CWE-732 and CWE-79 alongside CVE-2021-44228 and CVE-2019-0001",
        )
        adapted = self.adapt(object_form(record))
        self.assertEqual(len(adapted.rows), 1)
        row = adapted.rows[0]
        self.assertEqual(row["cwe"], "CWE-79", msg="79 is lower than 732")
        self.assertEqual(row["cve"], "CVE-2019-0001", msg="2019 is earlier than 2021")
        self.assertEqual(adapted.counters["multi_valued_cwe_records"], 1)
        self.assertEqual(adapted.counters["multi_valued_cve_records"], 1)

    def test_a_second_location_is_counted_and_never_reaches_the_path(self) -> None:
        """The first-location rule: the row takes the primary location, the record counts once.

        ``caller_file_path`` is the only second-location shape Checkov emits, and AAP 0.5.4's
        representation decision applies to it -- the row takes the first location, the record
        still counts once, and the number of multi-location records is reported per tool.
        """
        root = recorded_env().root
        record = anchored_check(
            SPARK_DOCKERFILE, root, caller_file_path=f"/{R_DOCKERFILE}"
        )
        adapted = self.adapt(object_form(record))
        self.assertEqual(len(adapted.rows), 1, msg="one record, one row")
        self.assertEqual(adapted.raw_records, 1, msg="and one counted record")
        self.assertEqual(adapted.rows[0]["path"], SPARK_DOCKERFILE)
        self.assertNotEqual(
            adapted.rows[0]["path"], R_DOCKERFILE, msg="the caller location is not the path"
        )
        self.assertEqual(adapted.counters["multi_location_records"], 1)

    def test_no_row_carries_an_absolute_path_or_an_absent_required_field(self) -> None:
        """Brief assertion 19, measured through the emitter's own rules on all seven fixtures."""
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertSchemaClean(adapted.rows, context=stem)
                for row in adapted.rows:
                    self.assertIsNotNone(row["severity_norm"])
                    self.assertIsNotNone(row["path"])
                    self.assertIn(row["severity_norm"], severity.SEVERITY_NORM)


# ======================================================================================
# 6. Severity (brief assertion 16)
# ======================================================================================


class SeverityTests(CheckovAdapterTestCase):
    """``severity_native`` is ``null`` and ``severity_norm`` is ``Info``, with the basis stated.

    Severities require a licence and no credential is provisioned (AAP 0.3.2), so every row
    of every committed fixture carries the absence. What matters is that the absence is
    **stated** rather than a level assumed: the basis is recorded, the tally carries the
    absence as an entry, and no literal is fabricated to stand in for it.
    """

    def test_the_unlicensed_configuration_records_the_absence_rather_than_a_level(self) -> None:
        """``None`` / ``Info`` / ``no_vocabulary`` on every row, with the counters agreeing.

        ``severity_absent`` equalling the row count exactly is the checkable form of the
        unlicensed-configuration expectation, and ``severity_basis_no_vocabulary`` is what says
        the band came from policy rather than from something read.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for index, row in enumerate(adapted.rows):
                    self.assertIsNone(row["severity_native"], msg=f"{stem} row {index}")
                    self.assertEqual(row["severity_norm"], severity.INFO)
                    self.assertEqual(row["severity_norm"], "Info")
                self.assertEqual(adapted.counters["severity_present"], 0)
                self.assertEqual(adapted.counters["severity_absent"], len(adapted.rows))
                basis_key = f"{checkov.COUNTER_SEVERITY_BASIS_PREFIX}{severity.BASIS_NO_VOCABULARY}"
                self.assertEqual(adapted.counters[basis_key], len(adapted.rows))

    def test_the_tally_records_the_absence_and_fabricates_no_literal(self) -> None:
        """One entry, ``severity_native`` ``None``, basis ``no_vocabulary``, rows == the rows.

        The tally is ``severity-map.md``'s input, so an absence recorded as the string
        ``"None"``, as ``"Info"`` or as ``"UNKNOWN"`` would put a literal in that document
        that no record ever carried. Asserting the entry rather than only the row's fields is
        what catches that.
        """
        adapted = self.adapt_fixture(CAPTURED_FIXTURE)
        entries = adapted.tally.entries(checkov.TOOL)
        self.assertEqual(len(entries), 1, msg="one distinct literal -- the absence")
        entry = entries[0]
        self.assertIsNone(entry.severity_native, msg="the absence is recorded as an absence")
        self.assertEqual(entry.severity_norm, severity.INFO)
        self.assertEqual(entry.basis, severity.BASIS_NO_VOCABULARY)
        self.assertFalse(entry.unmapped, msg="an absence is not an unmapped literal")
        self.assertEqual(entry.rows, len(adapted.rows))
        self.assertEqual(adapted.tally.row_count(checkov.TOOL), len(adapted.rows))
        self.assertEqual(
            adapted.tally.unmapped_by_tool()[checkov.TOOL],
            (),
            msg="nothing is disclosed as unmapped when nothing was observed",
        )
        self.assertEqual(
            adapted.tally.band_counts(checkov.TOOL)[severity.INFO], len(adapted.rows)
        )

    def test_the_tally_is_fed_once_per_emitted_row_and_never_for_a_rejection(self) -> None:
        """A rejected record contributes no row, so it must contribute no tallied literal.

        Otherwise ``severity-map.md`` would count a literal against rows the dataset does not
        contain. The reject fixtures are the only inputs where the two numbers can diverge.
        """
        for stem in REJECT_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertGreater(len(adapted.rejections), 0, msg="a real negative fixture")
                self.assertEqual(adapted.tally.row_count(checkov.TOOL), len(adapted.rows))

    def test_a_licensed_label_is_mapped_through_the_case_insensitive_table(self) -> None:
        """Every label in ``severity.label_table()``, mapped to its band, case-insensitively.

        The licensed configuration is not this run's, but the mapping is policy fixed in
        advance rather than tuned to what an artifact turned out to contain -- so it is
        asserted here against the table itself. Each literal reaches ``severity_native``
        exactly as written, which is what keeps ``severity-map.md``'s per-literal counts
        counts of what was observed.
        """
        root = recorded_env().root
        table = severity.label_table()
        self.assertIn("CRITICAL", table)
        self.assertIn("NEGLIGIBLE", table)
        for literal, band in sorted(table.items()):
            for spelling in (literal, literal.lower(), literal.title()):
                with self.subTest(literal=spelling):
                    adapted = self.adapt(
                        object_form(
                            anchored_check(SPARK_DOCKERFILE, root, severity=spelling)
                        )
                    )
                    self.assertEqual(len(adapted.rows), 1)
                    row = adapted.rows[0]
                    self.assertEqual(
                        row["severity_native"],
                        spelling,
                        msg="the literal is recorded as observed, neither upper-cased nor "
                        "otherwise normalized",
                    )
                    self.assertEqual(row["severity_norm"], band)
                    self.assertEqual(adapted.counters["severity_present"], 1)
                    self.assertEqual(adapted.counters["severity_absent"], 0)
                    basis_key = (
                        f"{checkov.COUNTER_SEVERITY_BASIS_PREFIX}{severity.BASIS_LABEL}"
                    )
                    self.assertEqual(adapted.counters[basis_key], 1)

    def test_a_lowercase_label_and_its_uppercase_form_take_the_same_band(self) -> None:
        """``moderate`` and ``MODERATE`` are one weakness band and two distinct literals.

        Case folding is a measured requirement rather than a nicety: artifacts carry both
        spellings for the same band. The band is shared; the literal is preserved, so the two
        appear as two entries in the tally instead of one corrupted count.
        """
        root = recorded_env().root
        lower = self.adapt(
            object_form(anchored_check(SPARK_DOCKERFILE, root, severity="moderate"))
        )
        upper = self.adapt(
            object_form(anchored_check(SPARK_DOCKERFILE, root, severity="MODERATE"))
        )
        self.assertEqual(lower.rows[0]["severity_norm"], "Medium")
        self.assertEqual(upper.rows[0]["severity_norm"], "Medium")
        self.assertEqual(lower.rows[0]["severity_native"], "moderate")
        self.assertEqual(upper.rows[0]["severity_native"], "MODERATE")

    def test_a_literal_outside_every_mapped_vocabulary_is_disclosed_as_unmapped(self) -> None:
        """``Info`` with the literal recorded, never silently dropped and never guessed.

        AAP 0.5.4: a literal outside every mapped vocabulary maps to ``Info`` and is listed
        with the rows it affected. The disclosure is what makes the mapping auditable, so both
        the band and the ``unmapped_literal`` entry are asserted.
        """
        root = recorded_env().root
        literal = "CATASTROPHIC"
        self.assertNotIn(literal, severity.label_table(), msg="the literal must be unmapped")
        adapted = self.adapt(
            object_form(anchored_check(SPARK_DOCKERFILE, root, severity=literal))
        )
        row = adapted.rows[0]
        self.assertEqual(row["severity_native"], literal)
        self.assertEqual(row["severity_norm"], severity.INFO)
        basis_key = f"{checkov.COUNTER_SEVERITY_BASIS_PREFIX}{severity.BASIS_UNMAPPED_LITERAL}"
        self.assertEqual(adapted.counters[basis_key], 1)
        disclosed = adapted.tally.unmapped_by_tool()[checkov.TOOL]
        self.assertEqual(len(disclosed), 1)
        self.assertEqual(disclosed[0].severity_native, literal)
        self.assertEqual(disclosed[0].rows, 1)

    def test_severity_norm_is_never_absent_whatever_the_record_carries(self) -> None:
        """Five inputs, five bands, no absence -- ``severity_norm`` has no null path.

        ``path`` and ``severity_norm`` are the two fields absence is never permitted for
        (AAP 0.8.2), and this is the one of the pair a severity policy could get wrong.
        """
        root = recorded_env().root
        for literal in (None, "", "   ", "HIGH", "not-a-vocabulary"):
            with self.subTest(severity=literal):
                record = anchored_check(SPARK_DOCKERFILE, root, severity=literal)
                adapted = self.adapt(object_form(record))
                self.assertEqual(len(adapted.rows), 1)
                self.assertIn(adapted.rows[0]["severity_norm"], severity.SEVERITY_NORM)
                self.assertIsNotNone(adapted.rows[0]["severity_norm"])


# ======================================================================================
# 7. The negative fixtures -- one per rejection condition this adapter can produce
# ======================================================================================


class RejectionTests(CheckovAdapterTestCase):
    """Each rejection condition: no row, one counted rejection, the class asserted by name.

    A test that only counts rejections cannot tell one condition from another, so the class
    name carries the assertion and the diagnostic is compared verbatim against the
    expectation. Every one is present whether or not this run's own artifact contained the
    case (AAP 0.9.4): a rejection path with no test is a rejection path nobody has exercised.

    The surviving records in each fixture matter as much as the rejected ones. Each negative
    fixture places its defective record among sound ones -- in the middle, not at an end --
    so the rows that follow are the proof that the traversal carried on rather than
    discarding the rest of the artifact. AAP 0.5.4: partial parse is a first-class outcome.
    """

    def test_each_negative_fixture_produces_exactly_its_expected_rejections(self) -> None:
        """Class by name, diagnostic verbatim, record identity, and the counts around them."""
        for stem in REJECT_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                expected = load_expected(stem)
                self.assertEqual(
                    len(adapted.rejections),
                    len(expected["rejections"]),
                    msg=f"{stem}: rejection count -- observed {adapted.classes}",
                )
                for index, (rejection, expectation) in enumerate(
                    zip(adapted.rejections, expected["rejections"])
                ):
                    self.assertRejection(
                        rejection, expectation, context=f"{stem} rejection {index}"
                    )
                self.assertRowsMatchExpected(adapted, expected, context=stem)
                self.assertCountersMatchExpected(adapted, expected, context=stem)
                self.assertReconciles(adapted, expected, context=stem)
                self.assertSchemaClean(adapted.rows, context=stem)

    def test_a_rejected_record_produces_no_row_and_never_both(self) -> None:
        """One record, one outcome: a row or a rejection, never both and never neither.

        Established as arithmetic over the whole artifact rather than record by record,
        because that is the form the reconciliation identity takes -- and the identity is what
        would break silently if a record ever produced two outcomes or none.
        """
        for stem in REJECT_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertEqual(len(adapted.rows) + len(adapted.rejections), adapted.raw_records)
                self.assertGreater(len(adapted.rejections), 0, msg="a real negative fixture")

    def test_the_traversal_continues_past_a_rejected_record(self) -> None:
        """Rows after the rejected record prove the rest of the artifact was still read.

        Each fixture places its defect where continuation is observable: a rejection at the
        end would establish nothing about what follows it.
        """
        for stem in REJECT_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertGreater(
                    len(adapted.rows),
                    0,
                    msg=f"{stem}: the sound records around the defect must still be rows",
                )

    def test_each_rejection_class_is_counted_under_its_own_name(self) -> None:
        """``rejections_by_class`` matches the expectation, class for class.

        The non-integer-start-line fixture is the one that matters most here: it carries two
        classes at once -- one element-type defect and three malformed containers -- so a test
        that only counted rejections would pass while the class boundary between them was
        wrong.
        """
        for stem in REJECT_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                expected = load_expected(stem)["counts"]["rejections_by_class"]
                self.assertEqual(adapted.by_class, expected)
                for reject_class in adapted.by_class:
                    self.assertIn(reject_class, paths.REJECT_CLASSES)

    def test_every_negative_condition_behaves_identically_in_the_array_form(self) -> None:
        """The shape is orthogonal to all five conditions, so both shapes must agree.

        No array-form negative fixture is committed, so the document is the loaded object-form
        report wrapped into a one-element array **in memory**. That changes no field value and
        writes no file -- and it is a different thing from the positive equivalence assertion,
        which uses the committed derived fixture precisely because deriving a shape in-test
        would test this file's arithmetic rather than the adapter's.
        """
        for stem in REJECT_FIXTURES:
            with self.subTest(fixture=stem):
                as_object = self.adapt(load_fixture(stem))
                as_array = self.adapt(array_form(load_fixture(stem)))
                self.assertEqual(as_object.rows, as_array.rows, msg=f"{stem}: rows")
                self.assertEqual(as_object.classes, as_array.classes, msg=f"{stem}: classes")
                self.assertEqual(
                    [rejection.detail for rejection in as_object.rejections],
                    [rejection.detail for rejection in as_array.rejections],
                    msg=f"{stem}: diagnostics",
                )
                self.assertEqual(
                    [dict(rejection.record_identity) for rejection in as_object.rejections],
                    [dict(rejection.record_identity) for rejection in as_array.rejections],
                    msg=f"{stem}: record identities",
                )
                self.assertEqual(as_object.raw_records, as_array.raw_records)
                self.assertEqual(as_array.counters["top_level_form_array"], 1)

    def test_the_malformed_element_is_never_salvaged_into_a_row(self) -> None:
        """The bare string whose value spells a check identifier is a trap, not a salvage.

        An adapter tempted to read it as a ``check_id`` would emit a row with a rule identifier
        and nothing else. No row of this fixture may carry that element's value as a
        ``rule_id`` at a location the element never named -- and the element is a string, so it
        names none.
        """
        stem = "reject-checkov-malformed-record"
        document = load_fixture(stem)
        adapted = self.adapt(document)
        strings = [record for record in failed_check_union(document) if isinstance(record, str)]
        self.assertTrue(strings, msg="the fixture must carry a non-object element")
        self.assertEqual(len(adapted.rejections), 2)
        self.assertEqual(
            adapted.classes, [paths.REJECT_MALFORMED_RECORD, paths.REJECT_MALFORMED_RECORD]
        )
        for row in adapted.rows:
            self.assertIsNotNone(row["path"], msg="every emitted row names a location")
            self.assertIsNotNone(row["message"], msg="and carries its own message")

    def test_a_record_defective_in_two_ways_takes_the_first_class_in_the_fixed_order(self) -> None:
        """The classification order is fixed, so a class is reproducible rather than incidental.

        Shape, then rule identifier, then message, then path, then ``start_line``. A record
        missing both its identifier and its message is ``missing_rule_id``; one missing its
        message and carrying an unusable line range is ``missing_message``. Without a fixed
        order the same record could be counted under either class from one run to the next.
        """
        root = recorded_env().root
        cases = (
            (
                "no identifier and no message",
                anchored_check(SPARK_DOCKERFILE, root, check_id=_OMIT, check_name=_OMIT),
                paths.REJECT_MISSING_RULE_ID,
            ),
            (
                "no message and an unusable line range",
                anchored_check(
                    SPARK_DOCKERFILE, root, check_name=_OMIT, file_line_range="not an array"
                ),
                paths.REJECT_MISSING_MESSAGE,
            ),
            (
                "no path and an unusable line range",
                anchored_check(
                    SPARK_DOCKERFILE,
                    root,
                    file_path=_OMIT,
                    repo_file_path=_OMIT,
                    file_abs_path=_OMIT,
                    file_line_range=["20", 40],
                ),
                paths.REJECT_ABSENT_PATH,
            ),
            (
                "not an object at all, with everything else wrong too",
                "CKV_DOCKER_2",
                paths.REJECT_MALFORMED_RECORD,
            ),
        )
        for label, record, expected_class in cases:
            with self.subTest(record=label):
                adapted = self.adapt(object_form(record))
                self.assertEqual(adapted.rows, [])
                self.assertEqual(adapted.classes, [expected_class], msg=label)

    def test_an_unusable_line_range_container_is_not_read_as_an_absence(self) -> None:
        """A structurally wrong container is ``malformed_record``; a genuine absence is neither.

        The distinction the non-integer-start-line fixture exists to hold: a ``file_line_range``
        that is present but not an array, or an empty array, is a defect, while an absent range
        or an explicitly null first element is the absence convention and produces a row. Read
        the first as "no line information" and the line of every record in a malformed artifact
        vanishes without a trace.
        """
        root = recorded_env().root
        rejected = {
            "not an array": "not an array",
            "an empty array": [],
            "a number": 36,
        }
        for label, value in rejected.items():
            with self.subTest(range=label):
                adapted = self.adapt(
                    object_form(anchored_check(SPARK_DOCKERFILE, root, file_line_range=value))
                )
                self.assertEqual(adapted.rows, [], msg=label)
                self.assertEqual(adapted.classes, [paths.REJECT_MALFORMED_RECORD], msg=label)

        permitted = {
            "the key absent": _OMIT,
            "an explicit null": None,
            "a null first element": [None, 48],
        }
        for label, value in permitted.items():
            with self.subTest(range=label):
                adapted = self.adapt(
                    object_form(anchored_check(SPARK_DOCKERFILE, root, file_line_range=value))
                )
                self.assertEqual(len(adapted.rows), 1, msg=label)
                self.assertIsNone(adapted.rows[0]["start_line"], msg=label)
                self.assertEqual(adapted.rejections, [], msg=label)
                self.assertEqual(adapted.counters["start_line_absent"], 1, msg=label)

        for label, value in {"a string": "20", "a boolean": True, "below one": 0}.items():
            with self.subTest(first_element=label):
                adapted = self.adapt(
                    object_form(
                        anchored_check(SPARK_DOCKERFILE, root, file_line_range=[value, 48])
                    )
                )
                self.assertEqual(adapted.rows, [], msg=label)
                self.assertEqual(
                    adapted.classes, [paths.REJECT_NON_INTEGER_START_LINE], msg=label
                )


class ProducibleClassBoundaryTests(CheckovAdapterTestCase):
    """Which of the ten classes this adapter can reach, and why the other four it cannot.

    Documenting the boundary as data rather than prose lets it be asserted: the two sets must
    partition ``paths.REJECT_CLASSES`` exactly, so a class added to the closed set would have
    to be classified here rather than quietly falling outside both.
    """

    def test_the_two_sets_partition_the_closed_rejection_vocabulary(self) -> None:
        """Six producible plus four unreachable equals the ten canonical classes, disjointly."""
        producible = set(PRODUCIBLE_REJECT_CLASSES)
        unreachable = set(UNREACHABLE_REJECT_CLASSES)
        self.assertEqual(producible | unreachable, set(paths.REJECT_CLASSES))
        self.assertEqual(producible & unreachable, set())
        self.assertEqual(len(paths.REJECT_CLASSES), 10)
        for reject_class in paths.REJECT_CLASSES:
            self.assertTrue(paths.is_reject_class(reject_class))

    def test_each_unreachable_class_carries_the_reason_it_is_out_of_reach(self) -> None:
        """A named reason per class, so "not tested" is never confused with "not reachable"."""
        for reject_class, reason in UNREACHABLE_REJECT_CLASSES.items():
            with self.subTest(reject_class=reject_class):
                self.assertIn(reject_class, paths.REJECT_CLASSES)
                self.assertTrue(reason.strip(), msg="every exclusion states its reason")

    def test_no_committed_fixture_produces_an_unreachable_class(self) -> None:
        """The boundary asserted against behaviour, not only against the documentation.

        A fixture producing one of the four would mean either the adapter reached a class this
        file says it cannot, or the class boundary here is wrong -- and either is a finding.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for reject_class in adapted.classes:
                    self.assertNotIn(reject_class, UNREACHABLE_REJECT_CLASSES)
                    self.assertIn(reject_class, PRODUCIBLE_REJECT_CLASSES)

    def test_an_absent_package_coordinate_is_not_a_rejection_for_this_tool(self) -> None:
        """The one unreachable class most likely to be reached by mistake.

        ``package_coordinate`` is ``None`` on every row this adapter emits. AAP 0.5.4 makes an
        unformable coordinate a rejection condition for a *dependency-oriented* record only,
        and a misconfiguration is not one -- so the field's absence is permitted and the row
        stands.
        """
        root = recorded_env().root
        adapted = self.adapt(object_form(anchored_check(SPARK_DOCKERFILE, root)))
        self.assertEqual(len(adapted.rows), 1)
        self.assertIsNone(adapted.rows[0]["package_coordinate"])
        self.assertEqual(adapted.rejections, [])
        self.assertIn("package_coordinate", checkov.ABSENCE_PERMITTED_FIELDS)


# ======================================================================================
# 8. Caller faults -- raised, never absorbed into a rejection count
# ======================================================================================


class CallerFaultTests(CheckovAdapterTestCase):
    """A defective *call* raises; a defective *record* is counted. The two never mix.

    Absorbing a caller fault into a rejection count would let a wrong root or another tool's
    path base produce a plausible dataset for a whole tool, and zero rows is indistinguishable
    from a clean scan. Each of these is validated once per call, before any record is read.
    """

    def test_another_tools_identifier_is_refused(self) -> None:
        """The identifier is required and validated, never ignored.

        Stamping another tool's name into these rows would misattribute every finding in the
        artifact, and the field is the only thing in the dataset that says which tool produced
        a row.
        """
        with self.assertRaises(checkov.CheckovAdapterError):
            self.adapt(object_form(), tool="trivy")
        with self.assertRaises(checkov.CheckovAdapterError):
            self.adapt(object_form(), tool=None)

    def test_a_relative_or_non_text_root_is_refused(self) -> None:
        """A relative root cannot anchor anything, so it fails on the call rather than per row.

        Accepting one would produce a plausible-looking wrong answer for every row, which is
        the failure mode hardest to notice.
        """
        for root in ("relative/spark-src", "", b"/opt/spark-src", 17):
            with self.subTest(root=root):
                with self.assertRaises(checkov.CheckovAdapterError):
                    self.adapt(object_form(), root=root)

    def test_another_tools_path_base_is_refused(self) -> None:
        """A foreign base would resolve every path against the wrong thing, silently.

        Which is exactly what AAP 0.5.4's requirement that every base come from the recorded
        runner metadata exists to prevent.
        """
        foreign = paths.ToolPathBase(
            tool="gitleaks",
            kind=paths.PATH_BASE_KIND_SCAN_ROOT,
            base_value=RECORDED_SCAN_ROOT,
            scan_root=RECORDED_SCAN_ROOT,
        )
        with self.assertRaises(checkov.CheckovAdapterError):
            self.adapt(object_form(), tool_base=foreign)
        with self.assertRaises(checkov.CheckovAdapterError):
            self.adapt(object_form(), tool_base=None)

    def test_an_allowlist_that_is_a_bare_string_is_refused(self) -> None:
        """A string would be iterated character by character, putting every row out of scope."""
        with self.assertRaises(checkov.CheckovAdapterError):
            self.adapt(object_form(), allowlist="core/src/main/**")
        with self.assertRaises(checkov.CheckovAdapterError):
            self.adapt(object_form(), allowlist=None)

    def test_a_tally_that_cannot_record_is_refused(self) -> None:
        """Every row's literal has to reach ``severity-map.md``, including an absence.

        A silently skipped tally would leave that document under-reporting with nothing to
        show it had.
        """
        with self.assertRaises(checkov.CheckovAdapterError):
            self.adapt(object_form(), tally=None)
        with self.assertRaises(checkov.CheckovAdapterError):
            self.adapt(object_form(), tally=object())

    def test_a_top_level_that_is_neither_shape_raises_rather_than_returning_nothing(self) -> None:
        """Neither an array nor an object: a mis-route, and it must not read as a clean scan.

        Returning zero rows here would be indistinguishable from an artifact that found
        nothing, which is the whole reason shape detection halts on a document matching no
        known shape rather than parsing it best-effort.
        """
        for document in ("a string", 17, 3.5, True, None):
            with self.subTest(document=document):
                with self.assertRaises(checkov.CheckovAdapterError):
                    self.adapt(document)

    def test_a_caller_fault_is_a_value_error_and_not_a_rejection(self) -> None:
        """``CheckovAdapterError`` is a ``ValueError`` subclass, and no rejection is produced.

        A ``ValueError`` subclass rather than an ``assert``, because ``python -O`` strips
        ``assert`` and an invariant that disappears under optimisation is not an invariant.
        """
        self.assertTrue(issubclass(checkov.CheckovAdapterError, ValueError))
        self.assertNotIsInstance(
            checkov.CheckovAdapterError("x"),
            paths.Rejection,
            msg="a caller fault is not a counted record",
        )

    def test_metadata_with_no_entry_for_this_tool_is_a_metadata_fault(self) -> None:
        """A missing base is surfaced, never defaulted to the root.

        AAP 0.6.1: missing metadata for a tool that wrote an artifact is a hard error the
        caller surfaces -- *guessing a base is exactly how every row for that tool gets a wrong
        path*. The error type is the metadata module's own, so the fault is attributable to the
        metadata rather than to a record.
        """
        directory = recorded_env().directory / "metadata-fault"
        directory.mkdir(parents=True, exist_ok=True)
        document_path = directory / "runner-metadata.json"
        document_path.write_text(
            json.dumps({"spark_src": RECORDED_SCAN_ROOT, "tools": {"gitleaks": {}}}),
            encoding="utf-8",
        )
        document = paths.load_runner_metadata(document_path)
        with self.assertRaises(paths.RunnerMetadataError):
            paths.tool_path_base(document, checkov.TOOL)


class RootContainmentTests(CheckovAdapterTestCase):
    """A ``..`` anywhere in a coordinate takes it out of the tree, not only at the front.

    These assertions are about ``normalize.paths``' containment rule rather than about
    Checkov's fields, and they live in this module because this is the module that owns the
    path-trap material: the user's worked example in AAP 0.5.3 is *"reading that slash as
    filesystem-absolute produces a long ``../`` path and a false ``in_scope: false``"*, and
    the rule tested here is the same failure in the other direction -- a coordinate that
    leaves the tree read as a file inside it, with a true ``in_scope: true`` on a glob it
    matches only on its leading segments. This module already drives ``path_kind_for``,
    ``in_scope`` and ``scope_decision`` directly and already builds records carrying hostile
    paths, so the assertions have one home rather than two.

    The emitted spelling is asserted **unchanged** throughout. The SARIF 2.1.0 errata (the
    section 3.10.2 amendment) forbid a consumer normalizing ``..`` out of a path and AAP
    0.5.4 requires ``../`` segments preserved, so containment governs the *classification*
    only, and nothing about the string that reaches the dataset.
    """

    #: Three concrete segments and four ``..``, so the running depth is spent by the third
    #: and the fourth takes it below the root. Its leading segments are exactly
    #: ``core/src/main``, so a classification that read only the first segment would call it
    #: a tree file while ``core/src/main/**`` matched it -- which is why the verdict is a
    #: walk over every segment rather than a test of the leading one.
    INTERIOR_ESCAPE = "core/src/main/../../../../etc/passwd"

    #: An interior ``..`` that stays inside the tree: a real sibling-directory reference.
    #: It must remain in scope, which is the half of the rule a blunt "any ``..`` is an
    #: escape" implementation would get wrong.
    INTERIOR_PARENT_INSIDE = "core/src/main/scala/../java/A.java"

    #: An interior ``..`` whose shadow lands in a test tree. The reported spelling carries
    #: ``src/main``; only the shadow carries the literal ``src/test``.
    INTERIOR_PARENT_INTO_TEST = "sql/core/src/main/../test/A.scala"

    #: A shadow that lands *inside* an allowlist root from a first segment that is not one.
    #: The reported spelling matches no glob; the coordinate lexically names a file under
    #: ``core/src/main``.
    SHADOW_INTO_SCOPE = "unrelated/../core/src/main/scala/A.scala"

    #: An archive member whose container leaves the tree, in the shape AAP 0.5.4 defines.
    ESCAPING_ARCHIVE = "a/../../guava-18.0.jar!META-INF/maven/com.google.guava/pom.xml"

    #: The same shape as it actually occurs, taken from the dependency-check artifact's
    #: Maven-repository coordinate: a leading chain rather than an interior one.
    LEADING_ARCHIVE = (
        "../../root/.m2/repository/com/google/guava/guava/18.0/guava-18.0.jar"
        "!META-INF/maven/com.google.guava/guava/pom.xml"
    )

    def test_an_interior_parent_chain_escapes_the_root_and_is_out_of_scope(self) -> None:
        """An interior ``..`` chain: classified ``outside_root``, and ``in_scope`` is false.

        The glob match is demonstrated first -- ``core/src/main/**`` really does match this
        path's segments -- so the record shows that the containment walk is what excludes the
        coordinate and that no allowlist matcher could, since the coordinate is a legitimate
        match on its segments. AAP 0.5.4: every non-filesystem coordinate takes
        ``in_scope: false``, is kept, and is counted.
        """
        globs = recorded_env().globs
        self.assertTrue(
            paths.match_glob("core/src/main/**", self.INTERIOR_ESCAPE),
            msg=(
                "the premise of the finding: this coordinate matches the glob on its "
                "segments, so nothing but a containment test can exclude it"
            ),
        )
        analysis = paths.analyse_containment(self.INTERIOR_ESCAPE)
        self.assertTrue(analysis.escapes_root, msg="the running depth goes below the root")
        self.assertEqual(
            analysis.escaping_segment_index,
            6,
            msg="the escape is at the fourth '..', which is segment index 6",
        )
        self.assertEqual(analysis.minimum_depth, -1)
        self.assertEqual(analysis.final_depth, 1, msg="and it ends one segment deep, outside")
        self.assertEqual(analysis.canonical_path, "../etc/passwd")
        self.assertEqual(
            paths.path_kind_for(self.INTERIOR_ESCAPE),
            paths.PATH_KIND_OUTSIDE_ROOT,
            msg="a coordinate that leaves the tree is an outside-root coordinate",
        )
        self.assertFalse(
            paths.in_scope(self.INTERIOR_ESCAPE, globs),
            msg=(
                "and in_scope is false even with the default tree_file kind, so a caller "
                "that did not pass the resolved kind still cannot get a true verdict"
            ),
        )
        decision = paths.scope_decision(
            self.INTERIOR_ESCAPE, globs, kind=paths.PATH_KIND_OUTSIDE_ROOT
        )
        self.assertFalse(decision.in_scope)
        self.assertTrue(decision.excluded_as_escaping_root, msg=decision.reason())
        self.assertIsNone(
            decision.matched_glob,
            msg="an escaping coordinate is attributed to no allowlist root",
        )

    def test_an_interior_parent_that_stays_inside_the_root_remains_in_scope(self) -> None:
        """A sibling-directory reference is not an escape, and must keep its scope.

        The other half of the rule. ``..`` is not evidence of anything on its own: the
        verdict is the running depth, and this coordinate's depth never reaches zero. A
        implementation that excluded every path carrying ``..`` would drop real in-scope
        rows, which is the same silent-loss failure in the opposite direction.
        """
        globs = recorded_env().globs
        analysis = paths.analyse_containment(self.INTERIOR_PARENT_INSIDE)
        self.assertFalse(analysis.escapes_root)
        self.assertIsNone(analysis.escaping_segment_index)
        self.assertEqual(analysis.minimum_depth, 0, msg="the depth never goes negative")
        self.assertEqual(analysis.canonical_path, "core/src/main/java/A.java")
        self.assertEqual(
            paths.path_kind_for(self.INTERIOR_PARENT_INSIDE), paths.PATH_KIND_TREE_FILE
        )
        self.assertTrue(paths.in_scope(self.INTERIOR_PARENT_INSIDE, globs))
        decision = paths.scope_decision(self.INTERIOR_PARENT_INSIDE, globs)
        self.assertTrue(decision.in_scope, msg=decision.reason())
        self.assertFalse(decision.excluded_as_escaping_root)
        self.assertEqual(decision.matched_glob, "core/src/main/**")
        self.assertEqual(
            decision.matched_spelling,
            "reported",
            msg="it matches as reported, so the shadow is not consulted at all",
        )

    def test_an_archive_member_whose_container_escapes_is_seen_to_escape(self) -> None:
        """The container's depth decides, and the member's own ``..`` is not consulted.

        A member path moves within the archive; only the container can move relative to the
        scan root. The kind stays ``archive_member`` -- the more specific of two
        non-filesystem kinds, and the one the dependency-check expectation records for the
        Maven-repository coordinate -- while the analysis still reports the container's
        escape for any caller that needs it.
        """
        globs = recorded_env().globs
        for coordinate, expected_minimum in (
            (self.ESCAPING_ARCHIVE, -1),
            (self.LEADING_ARCHIVE, -2),
        ):
            with self.subTest(coordinate=coordinate):
                analysis = paths.analyse_containment(coordinate)
                self.assertTrue(analysis.is_archive_reference)
                self.assertTrue(
                    analysis.escapes_root, msg="the container leaves the scanned tree"
                )
                self.assertEqual(analysis.minimum_depth, expected_minimum)
                self.assertNotIn(
                    paths.ARCHIVE_SEPARATOR,
                    "".join(analysis.segments),
                    msg="only the container's segments are walked",
                )
                self.assertEqual(
                    paths.path_kind_for(coordinate),
                    paths.PATH_KIND_ARCHIVE_MEMBER,
                    msg="the archive test stays first; both kinds are non-filesystem",
                )
                self.assertFalse(
                    paths.in_scope(coordinate, globs, kind=paths.PATH_KIND_ARCHIVE_MEMBER)
                )
                self.assertFalse(
                    paths.in_scope(coordinate, globs),
                    msg=(
                        "and the escape alone excludes it, so a caller that lost the kind "
                        "cannot recover a true verdict"
                    ),
                )

    def test_an_archive_member_whose_container_is_inside_the_root_does_not_escape(self) -> None:
        """The control for the case above: a container in the tree is not an escape.

        Without it, "the escaping container is excluded" would be consistent with every
        archive member being reported as escaping, which would make the measurement
        ``run-record.md`` publishes meaningless.
        """
        coordinate = "core/src/main/x.jar!org/apache/spark/Foo.class"
        analysis = paths.analyse_containment(coordinate)
        self.assertTrue(analysis.is_archive_reference)
        self.assertFalse(analysis.escapes_root)
        self.assertEqual(analysis.container, "core/src/main/x.jar")
        self.assertEqual(analysis.member, "org/apache/spark/Foo.class")
        self.assertEqual(
            paths.path_kind_for(coordinate), paths.PATH_KIND_ARCHIVE_MEMBER
        )
        self.assertFalse(
            paths.in_scope(coordinate, recorded_env().globs, kind=paths.PATH_KIND_ARCHIVE_MEMBER),
            msg="still out of scope, but on the non-filesystem rule rather than an escape",
        )

    def test_a_leading_parent_chain_is_classified_exactly_as_it_was(self) -> None:
        """The pre-existing case must not move: a leading ``..`` is still ``outside_root``.

        The running-depth walk subsumes the first-segment test rather than replacing its
        answers -- a leading ``..`` takes the depth negative at index 0 -- and this asserts
        the subsumption over the value the Checkov mis-reading actually produces.
        """
        root = recorded_env().root
        mis_read = paths.relativize_to_root(f"/{SPARK_DOCKERFILE}", root)
        analysis = paths.analyse_containment(mis_read)
        self.assertEqual(
            analysis.escaping_segment_index, 0, msg="a leading '..' escapes at index 0"
        )
        self.assertEqual(
            paths.path_kind_for(mis_read),
            paths.PATH_KIND_OUTSIDE_ROOT,
            msg="the classification the first-segment test already gave",
        )
        self.assertFalse(paths.in_scope(mis_read, recorded_env().globs))

    def test_a_shadow_that_lands_inside_a_glob_is_in_scope_on_the_shadow(self) -> None:
        """``unrelated/../core/src/main/...`` is a file under ``core/src/main``, and says so.

        The mirror image of the escaping coordinate: the reported spelling matches no glob,
        so a matcher reading it alone answers ``in_scope: false`` for a coordinate that
        lexically names an in-scope file. The shadow is consulted only after the reported
        spelling fails, so the rule can add a match and never remove one, and which spelling
        matched is recorded rather than left for a reader to guess.
        """
        globs = recorded_env().globs
        analysis = paths.analyse_containment(self.SHADOW_INTO_SCOPE)
        self.assertFalse(analysis.escapes_root)
        self.assertTrue(analysis.canonical_differs)
        self.assertIsNone(
            paths.matches_any_glob(analysis.reported_path, globs),
            msg="the reported spelling matches nothing, which is the premise",
        )
        self.assertEqual(
            paths.matches_any_glob(analysis.canonical_path, globs), "core/src/main/**"
        )
        self.assertTrue(paths.in_scope(self.SHADOW_INTO_SCOPE, globs))
        decision = paths.scope_decision(self.SHADOW_INTO_SCOPE, globs)
        self.assertEqual(decision.matched_spelling, "canonical", msg=decision.reason())
        self.assertEqual(decision.canonical_path, "core/src/main/scala/A.scala")
        self.assertEqual(
            decision.path,
            self.SHADOW_INTO_SCOPE,
            msg="and the decision reports the reported spelling, never the shadow",
        )

    def test_a_shadow_that_lands_in_a_test_tree_is_excluded_on_the_literal_marker(self) -> None:
        """The ``src/test`` exclusion is tested on both spellings, or it is evadable.

        ``sql/core/src/main/../test/A.scala`` carries ``src/main`` as reported and
        ``src/test`` as shadowed. Testing the reported spelling alone would put a Scala test
        source in scope through a coordinate that names it by a different route, and AAP
        0.3.1's exclusion is a property of the location rather than of its spelling.
        """
        globs = recorded_env().globs
        analysis = paths.analyse_containment(self.INTERIOR_PARENT_INTO_TEST)
        self.assertNotIn(paths.SRC_TEST_MARKER, analysis.reported_path)
        self.assertIn(paths.SRC_TEST_MARKER, analysis.canonical_path)
        self.assertFalse(paths.in_scope(self.INTERIOR_PARENT_INTO_TEST, globs))
        decision = paths.scope_decision(self.INTERIOR_PARENT_INTO_TEST, globs)
        self.assertTrue(decision.excluded_by_src_test, msg=decision.reason())
        self.assertFalse(decision.in_scope)

    def test_the_analysis_rewrites_no_spelling_and_the_row_keeps_the_reported_path(self) -> None:
        """End to end: an escaping record is kept, byte-identical, and out of scope.

        The row is emitted rather than rejected -- AAP 0.5.4 keeps a non-filesystem
        coordinate and counts it -- its ``path`` is the reported spelling character for
        character with every ``..`` intact, and the adapter's own non-filesystem counter
        moves. Asserted through ``checkov.adapt`` rather than through ``paths`` alone,
        because the requirement is about what reaches the dataset.
        """
        root = recorded_env().root
        record = failed_check(
            check_id=SYNTHETIC_CHECK_ID,
            check_name=SYNTHETIC_CHECK_NAME,
            file_path=f"/{self.INTERIOR_ESCAPE}",
            repo_file_path=f"/{self.INTERIOR_ESCAPE}",
            file_abs_path=f"{root}/{self.INTERIOR_ESCAPE}",
            file_line_range=[1, 1],
        )
        adapted = self.adapt(object_form(record))
        self.assertEqual(adapted.rejections, [], msg="kept, not rejected")
        self.assertEqual(len(adapted.rows), 1)
        row = adapted.rows[0]
        self.assertEqual(
            row["path"],
            self.INTERIOR_ESCAPE,
            msg="the emitted spelling is the reported one, with every '..' preserved",
        )
        self.assertFalse(row["in_scope"])
        self.assertEqual(adapted.counters["rows_out_of_scope"], 1)
        self.assertEqual(adapted.counters["rows_in_scope"], 0)
        self.assertEqual(
            adapted.counters["non_filesystem_paths"],
            1,
            msg="an escaping coordinate is counted in the proportion run-record.md reports",
        )
        self.assertEqual(
            adapted.tally is not None,
            True,
            msg="the severity tally is still the caller's, unchanged by the path verdict",
        )

    def test_every_committed_fixture_row_is_unaffected_by_the_containment_rule(self) -> None:
        """The containment rule reaches no committed row's spelling or scope verdict.

        Every row of every fixture carries no ``..`` at all, so its shadow is its reported
        spelling and its verdict is the plain allowlist match. That is what bounds the
        containment rule to escaping coordinates: it is asserted here over every committed
        fixture rather than argued from the rule's shape.
        """
        globs = recorded_env().globs
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for index, row in enumerate(adapted.rows):
                    analysis = paths.analyse_containment(row["path"])
                    self.assertFalse(
                        analysis.canonical_differs,
                        msg=f"{stem} row {index}: {row['path']!r} has one spelling",
                    )
                    self.assertFalse(analysis.escapes_root, msg=f"{stem} row {index}")
                    self.assertEqual(
                        row["in_scope"],
                        paths.in_scope(row["path"], globs, kind=paths.PATH_KIND_TREE_FILE),
                        msg=f"{stem} row {index}: the verdict is the unchanged one",
                    )

    def test_the_walk_is_bounded_by_the_segment_count_and_consults_no_filesystem(self) -> None:
        """Cost is exactly the segment count, and the answer does not depend on the disk.

        ``Path.resolve`` and ``os.path.realpath`` would answer containment by following
        symlink chains whose length is no property of the input, and ``os.path.normpath``
        would collapse the ``..`` the errata protect. This walk is one pass, so the bound is
        a number the analysis itself reports -- asserted here on a 4,000-segment coordinate,
        which returns rather than recursing -- and the verdict for a path that exists on disk
        is identical to the verdict for one that does not.
        """
        for coordinate, expected in (
            ("a/b/c.txt", 3),
            (self.INTERIOR_ESCAPE, 9),
            (self.LEADING_ARCHIVE, 11),
            ("./a/./b", 3),
        ):
            with self.subTest(coordinate=coordinate):
                self.assertEqual(
                    paths.analyse_containment(coordinate).segments_walked, expected
                )
        deep = "/".join(["a"] * 2000 + [".."] * 2000)
        analysis = paths.analyse_containment(deep)
        self.assertEqual(analysis.segments_walked, 4000)
        self.assertFalse(analysis.escapes_root, msg="2,000 up from 2,000 down stays at the root")
        self.assertEqual(analysis.canonical_path, ".", msg="and its shadow is the root itself")
        materialised = tree_env()
        existing = f"{SPARK_DOCKERFILE}/../../x/../../../../../../../../../../../../etc/passwd"
        self.assertTrue(
            (Path(materialised.root) / SPARK_DOCKERFILE).exists(),
            msg="the first segment chain really is a file on disk in this environment",
        )
        self.assertTrue(
            paths.analyse_containment(existing).escapes_root,
            msg="an escape through a path that exists is still an escape",
        )
        self.assertTrue(
            paths.analyse_containment(
                "does/not/exist/../../../../../../etc/passwd"
            ).escapes_root,
            msg="and the verdict for a path that does not exist is the same",
        )

    def test_a_non_string_coordinate_is_a_policy_error_rather_than_a_classification(self) -> None:
        """Reject rather than infer: a non-string coordinate has no kind to report.

        The analysis raises, so the caller turns it into a counted ``malformed_record``
        rejection instead of receiving a plausible ``tree_file``. Asserted on all three
        entry points, because a guard on one of them is not a guard.
        """
        for value in (None, 5, ["a"], {"path": "a"}):
            with self.subTest(value=value):
                with self.assertRaises(paths.PathPolicyError):
                    paths.analyse_containment(value)
                with self.assertRaises(paths.PathPolicyError):
                    paths.path_kind_for(value)
                with self.assertRaises(paths.PathPolicyError):
                    paths.in_scope(value, recorded_env().globs)

    def test_the_containment_record_is_deterministic_and_json_serialisable(self) -> None:
        """``as_dict`` is a fixed key order and plain types, so it can enter a record.

        A rejection detail or a ``run-record.md`` note may need to say *where* a coordinate
        left the tree; that is only worth having if the serialisation is stable, so the key
        order is asserted as a list rather than a set and the whole dict is round-tripped
        through JSON.
        """
        analysis = paths.analyse_containment(self.INTERIOR_ESCAPE)
        record = analysis.as_dict()
        self.assertEqual(
            list(record),
            [
                "reported_path",
                "canonical_path",
                "escapes_root",
                "escaping_segment_index",
                "minimum_depth",
                "final_depth",
                "segments_walked",
                "container",
                "member",
            ],
        )
        self.assertEqual(json.loads(json.dumps(record)), record)
        self.assertEqual(
            paths.analyse_containment(self.INTERIOR_ESCAPE).as_dict(),
            record,
            msg="the same coordinate analysed twice gives the same record",
        )


# ======================================================================================
# 4. Failures only, and parsing errors as status evidence (brief assertions 11-13)
# ======================================================================================
