"""Field-by-field assertions over the Trivy 0.74.0 native adapter.

What this module tests
---------------------
``harness/lib/normalize/adapters/trivy.py``, the one adapter in this dataset whose
``scanner_class`` varies **row by row** and the only one carrying a structural
**halt**.  AAP 0.6.1 gives this file its row -- *"Asserts scanner_class derives from
the section array, never from record content, and that a non-empty unsupported
finding section halts rather than being dropped"* -- AAP 0.5.4 fixes the behaviour it
asserts, and AAP 0.9.4 puts it in the definition of done.  A failure here is one of
the conditions that stops the run (AAP 0.9.2), so nothing below is weakened to make a
test pass.

No user-specified rule governs this file.  ``review_rules`` returns exactly one line,
``No user rules provided.``, and that line is the whole document -- corroborated by
AAP 0.7 and AAP 0.10.2.  Enterprise best practice applies in their place and the
absence is not licence to lower the bar, which here means three concrete things: the
halt is asserted by its exact exception type rather than by a bare ``Exception``,
``scanner_class`` is asserted per row against the section the record was read from
rather than in aggregate, and every rejection is asserted by its **class name** taken
from :data:`normalize.paths.REJECT_CLASSES` rather than by a rejection count.  A test
that only counts rejections cannot tell one condition from another.

Why the unsupported-section halt has to be a halt and not a warning
-------------------------------------------------------------------
This is the whole justification for stopping the run, and it is the reason
:class:`~normalize.adapters.trivy.UnsupportedTrivySection` is asserted by type here
rather than tolerated:

**A silently dropped finding section would let reconciliation pass while real tool
output vanished.**  The reconciliation identity is
``raw finding records = dataset rows + rejected records``, and its left-hand side is
``reconcile._count_trivy``, whose count unit is ``Results[]`` x the **three
supported** sections.  An ignored ``Licenses`` array is therefore absent from *both*
sides: the identity would balance exactly -- over a count unit that never saw the
dropped array -- while four records of real tool output left no trace in the dataset,
in the counters, or in the reconciliation.  A drop that unbalances the identity is
caught by the identity; a drop invisible to both sides is caught by nothing, and an
empty or silently-reduced result set is indistinguishable from a clean scan.  Hence
AAP 0.5.4's requirement that the run stop with the observed structure quoted, and
hence :class:`UnsupportedSectionStopsTheRunTest` below, which also asserts the
counterfactual arithmetic that would have balanced.

Rejection conditions this adapter can produce, one negative fixture each
-----------------------------------------------------------------------
Seven of the ten classes in :data:`normalize.paths.REJECT_CLASSES` are reachable from
a Trivy artifact, and every one has a committed fixture asserted below whether or not
this run's own artifact contained the case (AAP 0.9.4): ``unresolvable_path``,
``missing_rule_id``, ``missing_message``, ``non_integer_start_line``,
``unattributable_section``, ``unformable_package_coordinate`` and
``malformed_record``.

Conditions this adapter **cannot** produce, and why
---------------------------------------------------
Stated so their absence is a recorded fact rather than a gap:

* **a cyclic, over-deep or invalid** ``uriBaseId`` **chain** (``invalid_uri``, and the
  chain terminal cases) -- a Trivy native report carries no SARIF base map at all.
  There is no ``uri``, no ``uriBaseId`` and no ``originalUriBaseIds`` for a chain to
  be walked over, so no chain can cycle, exceed a depth or be syntactically invalid.
  Those belong to the shared SARIF adapter and are asserted by
  ``test_sarif_adapter.py``;
* **an ambiguous bytecode-to-source resolution** (``ambiguous_source_resolution``) --
  this adapter resolves reported paths, never bytecode.  Its input is a filesystem
  report, there is no class identifier to resolve against ``src/main`` and
  ``src/test``, and therefore nothing that two source files could both claim.  That
  belongs to the Joern adapter and is asserted by ``test_joern_adapter.py``;
* ``absent_path`` -- not exercised here.  A Trivy record's path comes from the
  enclosing ``Results[].Target``, and the shape in which it goes missing in this
  corpus is the unresolvable one rather than the absent one.  It remains a member of
  :data:`normalize.paths.REJECT_CLASSES` and is not claimed by this file.

Hermetic by construction
------------------------
Every test runs against an absolute scan root inside one
:class:`tempfile.TemporaryDirectory`, an allowlist file holding the twelve
authoritative globs, and minimal ``runner-metadata.json`` documents -- all loaded
through ``paths.py``'s own loaders (:func:`normalize.paths.load_allowlist`,
:func:`normalize.paths.load_runner_metadata`, :func:`normalize.paths.tool_path_base`)
rather than fabricated, so the loaders are exercised on exactly the route ``cli.py``
uses.  No file is materialised inside that root: no code path under test reads the
tree, which is what makes the temporary root legitimate rather than a convenience.

Nothing outside that directory is written.  Every committed fixture is **read** and
never modified: :func:`load_fixture` re-parses the file on each call and every
derived variant is a :func:`copy.deepcopy`, and :class:`FixtureIntegrityTest` records
each fixture's sha256 at module setup and asserts it again afterwards.  This module
writes no deliverable -- in particular never ``oss-scan-results/findings.json`` or
``findings.csv``.

Presence is observed, never assumed
-----------------------------------
The precedent provisioning wrote **no Trivy artifact at all**, so nothing here reads
``harness/artifacts/raw/trivy.json`` or assumes it exists.  Every assertion is over a
committed fixture, and a missing fixture is reported as a blocking gap by
:class:`FixtureCorpusTest` rather than skipped silently -- a skipped test is a green
suite that asserted nothing.

Prohibitions this module observes (AAP 0.3.2, AAP 0.8.2)
--------------------------------------------------------
**No cross-tool interpretation of any kind.**  Trivy's three sections are not
compared with any other tool's coverage, in an assertion or in a comment; no other
tool's rows, counts or capabilities appear anywhere in this file.  Nothing is compared
against Apex, Cantina or any other scanner.  No finding is judged real, important, a
false positive or a duplicate, and nothing is deduplicated: two records naming the
same package at the same version are two rows and this file adds no remark about
them.  **No secret value appears in any fixture, literal, assertion message or
docstring** -- this tree is committed to git, whose only relevant ignore line matches
``artifacts/`` (``.gitignore:31``), so the guarantee is asserted structurally by
:class:`SecretRedactionTest` rather than left incidental.  Nothing under
``harness/lib/normalize/`` is edited by this file; a defect found there is reported,
never repaired here.

Imports and the ``sys.path`` bootstrap
--------------------------------------
Standard library only -- no ``pytest``, no third-party package, no manifest and no
install step (AAP 0.4.1, AAP 0.4.3) -- and the module runs under
``python3 -m unittest`` from any working directory.

Public API
    Every :class:`unittest.TestCase` below, discovered by ``unittest``.  The module's
    own helpers -- :class:`Environment`, :func:`load_fixture`, :func:`load_expected`,
    :func:`adapt_fixture`, :func:`section_walk` -- are shared fixtures for those
    tests, not a contract for anything outside this file.
"""

import copy
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
# namespace packages make "from normalize import ..." work once harness/lib is on
# sys.path. cli.py owns the same two lines for its own direct-script route and its
# docstring states that the test modules under oss-scan-results/adapter-tests/ reach
# these modules with them.
#
# This file sits at <repo>/oss-scan-results/adapter-tests/, so parents[2] is the
# repository root. The entry is derived from this file's own resolved location rather
# than from the working directory, which is what makes discovery work from the
# repository root and from anywhere else alike. It is inserted once and only if absent,
# so repeated imports and a discovery run that already put it there leave one entry.
# --------------------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from normalize import emit, paths, severity  # noqa: E402
from normalize.adapters import trivy  # noqa: E402

# --------------------------------------------------------------------------------------
# Locations. Both directories are inputs and are never written to by this module.
# --------------------------------------------------------------------------------------
ADAPTER_TESTS_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = ADAPTER_TESTS_DIR / "fixtures"
EXPECTED_DIR = ADAPTER_TESTS_DIR / "expected"

# --------------------------------------------------------------------------------------
# The twelve authoritative scope globs (AAP 0.3.1), byte-exact and in the request's
# order, with no exclusion line: the literal `src/test` exclusion is paths.py's, not the
# allowlist's.
#
# Restated here independently rather than read from paths.ALLOWLIST_GLOBS. The test
# writes these twelve lines to its own allowlist file, loads them back through
# paths.load_allowlist() and then confirms the loaded tuple is what paths.py authors, via
# paths.allowlist_matches_authoritative_globs(). Loading the module's own copy and
# comparing it with itself would assert nothing.
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

#: The canonical tool identifier every row this adapter emits must carry.
TOOL = "trivy"

#: The positive fixture: an unmodified captured-shape Trivy 0.74.0 filesystem report
#: carrying all three supported finding sections.
POSITIVE_FIXTURE = "trivy.json"

#: The fixture whose two appended elements each hold a non-empty unsupported finding
#: section. Its expected file records a stop rather than rows.
UNSUPPORTED_SECTION_FIXTURE = "halt-trivy-unsupported-section.json"

#: One negative fixture per rejection condition this adapter can produce (AAP 0.9.4).
#: The stems are the fixture and expected filenames alike, which is this folder's
#: convention: fixtures/<stem>.json against expected/<stem>.rows.json.
REJECT_FIXTURE_STEMS = (
    "reject-trivy-unresolvable-path",
    "reject-trivy-missing-rule-id",
    "reject-trivy-missing-message",
    "reject-trivy-non-integer-start-line",
    "reject-trivy-unattributable-section",
    "reject-trivy-no-package-coordinate",
    "reject-trivy-malformed-record",
)

#: The rejection classes those seven fixtures assert, so the mapping from fixture to
#: class is data rather than a string buried in a test. Each value is asserted to be a
#: literal member of paths.REJECT_CLASSES and to equal the module's own constant.
REJECT_CLASS_BY_STEM = {
    "reject-trivy-unresolvable-path": paths.REJECT_UNRESOLVABLE_PATH,
    "reject-trivy-missing-rule-id": paths.REJECT_MISSING_RULE_ID,
    "reject-trivy-missing-message": paths.REJECT_MISSING_MESSAGE,
    "reject-trivy-non-integer-start-line": paths.REJECT_NON_INTEGER_START_LINE,
    "reject-trivy-unattributable-section": paths.REJECT_UNATTRIBUTABLE_SECTION,
    "reject-trivy-no-package-coordinate": paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE,
    "reject-trivy-malformed-record": paths.REJECT_MALFORMED_RECORD,
}

#: The one fixture that is an unmerged per-directory part rather than the merged
#: artifact, and must be read with per_section_target semantics and an explicit
#: section base.
#:
#: Every other Trivy fixture states ArtifactName "." with each Target already prefixed
#: by its scope directory -- the merged shape run-trivy.sh writes. This one states its
#: own scope directory as ArtifactName with Targets relative to it, which is the shape
#: of one of the eighteen retained per-directory reports under logs/trivy.parts/.
#: runner-metadata.json records of those parts that they are not root-anchored, and
#: paths.resolve_trivy_path states the same from the resolver's side: a caller reading
#: one "passes a per_section_target base and a section_base; passing neither is a
#: rejection rather than a silent reading against the root." The base is therefore read
#: as input, never inferred from the artifact (AAP 0.5.4).
PER_SECTION_TARGET_STEMS = frozenset({"reject-trivy-unresolvable-path"})

#: The fixture whose outcome is reached through the public section-attribution seam,
#: trivy.adapt_record, because adapt's own iteration is section-bound by construction
#: and cannot hand a record a section outside SUPPORTED_SECTIONS. The adapter's own
#: docstring says the branch "is exercised by calling this function directly, which is
#: what the adapter test does".
SEAM_STEM = "reject-trivy-unattributable-section"

#: The record that seam hands over under a foreign section key, as the triple
#: (result_index, section-in-the-document, record_index), and the key it is handed
#: under. "Packages" is a member of trivy.KNOWN_NON_FINDING_KEYS and is physically
#: present as a non-empty object array on the very element the record sits in, so it is
#: the realistic mis-read rather than an invented string.
SEAM_DESIGNATED_RECORD = (3, "Vulnerabilities", 0)
SEAM_SECTION_KEY = "Packages"

#: The two path-base readings this module needs, both written into runner metadata
#: documents and loaded back through paths.py. The kinds are the ones
#: harness/artifacts/logs/runner-metadata.json records for this provisioning: scan_root
#: for the merged artifact, and per_section_target for the retained parts.
BASE_KIND_MERGED = paths.PATH_BASE_KIND_SCAN_ROOT
BASE_KIND_PART = paths.PATH_BASE_KIND_PER_SECTION_TARGET

#: The label vocabulary and the band each literal takes, restated independently of
#: severity.py so the comparison is against a written policy rather than against the
#: module's own copy of it (AAP 0.5.4's severity table; the matching is
#: case-insensitive).
EXPECTED_LABEL_BANDS = {
    "CRITICAL": "Critical",
    "HIGH": "High",
    "MODERATE": "Medium",
    "MEDIUM": "Medium",
    "LOW": "Low",
    "NEGLIGIBLE": "Info",
    "INFO": "Info",
    "INFORMATIONAL": "Info",
    "UNKNOWN": "Info",
    "NONE": "Info",
}

#: The five bands severity_norm may take, in severity order.
EXPECTED_SEVERITY_NORM = ("Critical", "High", "Medium", "Low", "Info")

#: The five fields absence is permitted for, and no others (AAP 0.8.2). Restated so the
#: invariant is asserted against a written list as well as against emit.py's own.
EXPECTED_OPTIONAL_FIELDS = frozenset(
    {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
)


class Environment:
    """The hermetic inputs every test shares: a scan root, an allowlist, two path bases.

    All three are real files inside one temporary directory, and both configuration
    files are read back through ``paths.py``'s own loaders rather than handed to the
    adapter as literals -- so the loaders are exercised on the same route ``cli.py``
    uses.  Nothing is materialised inside the scan root: no code path under test reads
    the tree, so a root that exists and is empty is exactly as good as a populated one
    and considerably easier to reason about.

    Two runner-metadata documents are written, not one, because two of this folder's
    Trivy fixtures are different artifacts.  The merged report the runner writes is
    ``scan_root``-based; the retained per-directory parts are ``per_section_target``
    and carry no single base, which is the whole subject of the unresolvable-path
    fixture.  Reading one with the other's base is what the metadata's own note about
    the parts exists to prevent.

    Attributes:
        directory: The temporary directory holding everything this object created.
        root: The absolute scan root every path is expressed against.
        allowlist_path: The allowlist file written here, then loaded.
        globs: The twelve authoritative globs, as ``paths.load_allowlist`` returned
            them.
        metadata_paths: The two metadata documents written, by base kind.
        metadata: The two loaded metadata documents, by base kind.
    """

    def __init__(self, directory: Path) -> None:
        """Create the scan root and write, then load, the allowlist and the metadata."""
        self.directory = directory
        root_path = directory / "spark-src"
        root_path.mkdir(parents=True, exist_ok=True)
        self.root = str(root_path)

        # One glob per line, byte-exact, a trailing newline and nothing else. No
        # exclusion line: the `src/test` exclusion is paths.py's.
        self.allowlist_path = directory / "allowlist.txt"
        self.allowlist_path.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS), encoding="utf-8"
        )
        self.globs = paths.load_allowlist(self.allowlist_path)

        self.metadata_paths: dict[str, Path] = {}
        self.metadata: dict[str, Any] = {}
        for kind in (BASE_KIND_MERGED, BASE_KIND_PART):
            location = directory / f"runner-metadata-{kind}.json"
            location.write_text(
                json.dumps(self._metadata_document(kind), indent=1) + "\n",
                encoding="utf-8",
            )
            self.metadata_paths[kind] = location
            self.metadata[kind] = paths.load_runner_metadata(location)

    def _metadata_document(self, kind: str) -> dict[str, Any]:
        """Build the minimal document ``paths.load_runner_metadata`` accepts.

        Minimal is deliberate.  The document carries the base facts a resolver needs
        -- the recorded kind, its value where the kind has one, the resolved scan root
        and the invocation shape -- and nothing that would make this test a second copy
        of the run's own record.  ``runner-metadata.json`` is the normalizer's *input*
        and is never generated from an artifact (AAP 0.6.4).

        A ``per_section_target`` base records no value on purpose: that kind supplies no
        single base, which is why ``ToolPathBase.base_for_relative()`` returns ``None``
        for it and why a relative per-record path under it is a counted rejection rather
        than a silent reading against the root.
        """
        path_base: dict[str, Any] = {"kind": kind}
        if kind == BASE_KIND_MERGED:
            path_base["value"] = self.root
            path_base["evidence"] = (
                "The merged artifact's Targets are root-relative: the runner invokes "
                "trivy fs once per scope directory and its merge step prefixes every "
                "Target with that part's own ArtifactName."
            )
        else:
            path_base["value"] = None
            path_base["evidence"] = (
                "A retained per-directory part is not root-anchored: each states its "
                "Target relative to the single path its invocation was given, so it is "
                "read with a section base rather than against the root."
            )
        return {
            "purpose": (
                "Minimal runner metadata for the Trivy adapter test. Written and read "
                "inside a temporary directory; it is not the run's record."
            ),
            "spark_src": self.root,
            "tools": {
                TOOL: {
                    "canonical_tool_identifier": TOOL,
                    "script_classification": "runner",
                    "path_base": path_base,
                    "resolved_scan_root": self.root,
                    "invocation_form": {
                        "target_passing_style": (
                            "exactly one root-relative path per invocation, one "
                            "invocation per scope directory"
                        ),
                        "invocations_per_run": len(AUTHORITATIVE_GLOBS),
                    },
                    "working_directory": {"path": self.root},
                }
            },
        }

    def tool_base(self, kind: str = BASE_KIND_MERGED) -> paths.ToolPathBase:
        """Return the tool's recorded path base, taken from the loaded document."""
        return paths.tool_path_base(self.metadata[kind], TOOL)


#: Module-level state, built once in :func:`setUpModule` and released in
#: :func:`tearDownModule`. Held at module level because every test needs the same root:
#: rebuilding it per test would make each test's rows depend on a different temporary
#: path for no gain.
ENV: Environment | None = None
_TEMPORARY_DIRECTORY: tempfile.TemporaryDirectory | None = None

#: Every fixture this module requires, and its sha256 as read at module setup. The
#: digests are taken once so :class:`FixtureIntegrityTest` can prove no test mutated a
#: committed file, and are measured rather than written down: the assertion is that they
#: do not change during the run, not that they equal a value recorded here.
_FIXTURE_DIGESTS: dict[str, str] = {}


def required_fixture_names() -> tuple[str, ...]:
    """Every fixture filename this module reads, in a stable order."""
    return (
        POSITIVE_FIXTURE,
        UNSUPPORTED_SECTION_FIXTURE,
        *(f"{stem}.json" for stem in REJECT_FIXTURE_STEMS),
    )


def required_expected_names() -> tuple[str, ...]:
    """Every hand-verified expected filename this module reads, in a stable order."""
    return (
        "trivy.rows.json",
        "halt-trivy-unsupported-section.rows.json",
        *(f"{stem}.rows.json" for stem in REJECT_FIXTURE_STEMS),
    )


def _sha256(location: Path) -> str:
    """Return the hex sha256 of a file, read as bytes."""
    return hashlib.sha256(location.read_bytes()).hexdigest()


def setUpModule() -> None:
    """Create the temporary scan root and configuration, and digest every fixture."""
    global ENV, _TEMPORARY_DIRECTORY
    _TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory(prefix="blitzy-trivy-adapter-")
    ENV = Environment(Path(_TEMPORARY_DIRECTORY.name))
    _FIXTURE_DIGESTS.clear()
    for name in required_fixture_names():
        location = FIXTURES_DIR / name
        if location.is_file():
            _FIXTURE_DIGESTS[name] = _sha256(location)


def tearDownModule() -> None:
    """Release the temporary directory. Nothing this module wrote survives it."""
    global ENV, _TEMPORARY_DIRECTORY
    ENV = None
    if _TEMPORARY_DIRECTORY is not None:
        _TEMPORARY_DIRECTORY.cleanup()
        _TEMPORARY_DIRECTORY = None


def environment() -> Environment:
    """Return the module's environment, or fail loudly if it was never built."""
    if ENV is None:  # pragma: no cover - defended, unreachable under unittest
        raise RuntimeError(
            "setUpModule did not run: the hermetic environment is missing, so no "
            "assertion below would be measuring the adapter against a known root"
        )
    return ENV


def load_fixture(name: str) -> Any:
    """Parse one committed fixture. The file is read and never written.

    A fresh document is returned on every call, so a test that derives a variant can
    deep-copy it with no risk of a shared object being mutated between tests.

    Raises:
        FileNotFoundError: Naming the missing fixture as a blocking gap. A fixture this
            module needs and cannot find is reported, never skipped: a skipped test is
            a green suite that asserted nothing (AAP 0.9.4).
    """
    location = FIXTURES_DIR / name
    if not location.is_file():
        raise FileNotFoundError(
            f"blocking gap: the Trivy adapter test requires the committed fixture "
            f"{location} and it is absent. The adapter's behaviour cannot be asserted "
            "without it, and this is reported rather than skipped."
        )
    return json.loads(location.read_text(encoding="utf-8"))


def load_expected(stem: str) -> Any:
    """Parse one hand-verified expected file from ``expected/``.

    Raises:
        FileNotFoundError: Naming the missing expectation, for the same reason.
    """
    location = EXPECTED_DIR / f"{stem}.rows.json"
    if not location.is_file():
        raise FileNotFoundError(
            f"blocking gap: the Trivy adapter test requires the hand-verified "
            f"expectation {location} and it is absent. Values asserted against nothing "
            "are not assertions, and this is reported rather than skipped."
        )
    return json.loads(location.read_text(encoding="utf-8"))


class Record:
    """One element of one supported finding section, with where it was read from.

    The section is carried alongside the record because the section is what decides the
    row's ``scanner_class``, and a walk that forgot it would be unable to state the
    expectation this file exists to assert.

    Attributes:
        result_index: The enclosing element's index in ``Results[]``.
        section: The section key the record was read from.
        record_index: The record's index within that section array.
        record: The record itself, as parsed.
        target: The enclosing ``Results[].Target``.
        element: The enclosing ``Results[]`` element.
    """

    __slots__ = ("result_index", "section", "record_index", "record", "target", "element")

    def __init__(
        self,
        result_index: int,
        section: str,
        record_index: int,
        record: Any,
        element: Any,
    ) -> None:
        """Hold one record and the coordinates that identify it."""
        self.result_index = result_index
        self.section = section
        self.record_index = record_index
        self.record = record
        self.element = element
        self.target = element.get("Target") if isinstance(element, dict) else None

    @property
    def pointer(self) -> str:
        """The JSON pointer naming this record, as the expected files spell it."""
        return f"/Results/{self.result_index}/{self.section}/{self.record_index}"

    @property
    def expected_scanner_class(self) -> str:
        """The class this record's row must take: its section's, and nothing else."""
        return trivy.SUPPORTED_SECTIONS[self.section]


def section_walk(document: Any) -> list[Record]:
    """Return every supported-section record, in the order ``adapt`` walks them.

    ``Results[]`` in document order, the three sections in the fixed order of
    :data:`trivy.SUPPORTED_SECTIONS` within each element, and each section's elements
    in order.  Section names are taken from that mapping rather than restated, so a
    change to the adapter's section set is visible here rather than silently untested.

    This walk is what makes the ``scanner_class`` expectation independent of the
    adapter: it reads which array each record sits in straight out of the document, so
    the assertion compares the emitted class against the document's own structure and
    not against another reading of the adapter's output.
    """
    found: list[Record] = []
    results = document.get("Results") if isinstance(document, dict) else None
    if not isinstance(results, list):
        return found
    for result_index, element in enumerate(results):
        if not isinstance(element, dict):
            continue
        for section in trivy.SUPPORTED_SECTIONS:
            records = element.get(section)
            if not isinstance(records, list):
                continue
            for record_index, record in enumerate(records):
                found.append(
                    Record(result_index, section, record_index, record, element)
                )
    return found


class Adapted:
    """One document's adaptation: its rows, rejections, counters and severity tally.

    Held together so a test asserts over one measurement rather than adapting the same
    document twice and comparing two.  A count that appears in two assertions must be
    one measurement cited twice (AAP 0.6.4).

    Attributes:
        document: The document adapted, as parsed or derived.
        rows: The dataset rows emitted, each carrying the twelve fields.
        rejections: The :class:`normalize.paths.Rejection` records counted instead.
        counters: The adapter's own counter mapping.
        tally: The :class:`normalize.severity.LiteralTally` fed once per emitted row.
        records: The document's supported-section records, in walk order.
    """

    __slots__ = ("document", "rows", "rejections", "counters", "tally", "records")

    def __init__(
        self,
        document: Any,
        rows: list[dict[str, Any]],
        rejections: list[paths.Rejection],
        counters: dict[str, int],
        tally: severity.LiteralTally,
    ) -> None:
        """Hold one adaptation and re-walk its document for the section expectation."""
        self.document = document
        self.rows = rows
        self.rejections = rejections
        self.counters = counters
        self.tally = tally
        self.records = section_walk(document)

    @property
    def rejections_by_class(self) -> dict[str, int]:
        """Rejection counts per named class, tallied as ``cli.py`` tallies them."""
        by_class: dict[str, int] = {}
        for rejection in self.rejections:
            by_class[rejection.reject_class] = by_class.get(rejection.reject_class, 0) + 1
        return by_class

    @property
    def raw_records(self) -> int:
        """The number of supported-section records the document holds.

        Counted by re-walking the document here rather than taken from the adapter's
        output: this is the left-hand side of the per-artifact identity, and taking it
        from the traversal that built the rows would satisfy the assertion while testing
        nothing (AAP 0.5.4).  The dataset-level identity and its own independent
        traversal, ``reconcile.count_records``, are ``test_reconciliation.py``'s
        subject; what is asserted here is that this adapter emits exactly one outcome
        per record it walked.
        """
        return len(self.records)


def adapt_document(
    document: Any,
    *,
    base_kind: str = BASE_KIND_MERGED,
    section_base: str | None = None,
) -> Adapted:
    """Adapt one document through ``trivy.adapt`` and hold the result.

    The keyword set is the uniform adapter entry point every module in this package
    exposes, so nothing here is Trivy-specific except the base kind a particular
    artifact shape requires and the section base that kind needs.

    A fresh tally per call: the tally is ``severity-map.md``'s input and no assertion
    here depends on state accumulated across documents.
    """
    env = environment()
    tally = severity.LiteralTally.with_all_tools()
    rows, rejections, counters = trivy.adapt(
        document,
        tool=TOOL,
        root=env.root,
        tool_base=env.tool_base(base_kind),
        allowlist=env.globs,
        tally=tally,
        section_base=section_base,
    )
    return Adapted(document, rows, rejections, counters, tally)


def adapt_fixture(stem: str) -> Adapted:
    """Adapt one committed fixture under the reading its own shape requires.

    The merged artifact is read against the recorded ``scan_root`` base.  The one
    unmerged per-directory part is read with a ``per_section_target`` base and a section
    base taken from the document's own ``ArtifactName`` -- the directory the invocation
    that produced it was given.  Both readings come from the recorded metadata and
    neither is inferred from the artifact's content.
    """
    document = load_fixture(f"{stem}.json")
    if stem in PER_SECTION_TARGET_STEMS:
        return adapt_document(
            document,
            base_kind=BASE_KIND_PART,
            section_base=document.get("ArtifactName"),
        )
    return adapt_document(document)


def adapt_through_seam(
    document: Any,
    *,
    designated: tuple[int, str, int],
    section_key: Any,
) -> Adapted:
    """Reproduce ``adapt``'s walk, handing one record a section key of our choosing.

    ``adapt``'s iteration is section-bound by construction -- it chooses one of
    :data:`trivy.SUPPORTED_SECTIONS`, counts the array and passes that section name
    down -- so no record it walks can arrive with a section outside the three.  The
    unattributable-section branch is therefore reached the way the adapter's own
    docstring says it is reached: by calling :func:`trivy.adapt_record` directly.

    Everything else about the walk is ``adapt``'s: pass one runs
    :func:`trivy.validate_finding_sections` over every element first, one counter
    mapping is threaded through, ``records_<section>`` is incremented by the length of
    each array at the moment the array is chosen -- which is what keeps the count unit
    identical to the counting traversal's -- and the element that held no record is
    counted.  Only the designated record's ``section`` argument differs, which is what
    isolates the guard from everything else the adapter does.
    """
    env = environment()
    tool_base = env.tool_base(BASE_KIND_MERGED)
    tally = severity.LiteralTally.with_all_tools()
    counters = trivy.new_counters()

    # Pass one, unchanged: a non-empty unsupported section anywhere would stop the run
    # before a single row existed, so this is a precondition of the expectation rather
    # than a detail.
    trivy.validate_finding_sections(document, tool=TOOL, counters=counters)

    rows: list[dict[str, Any]] = []
    rejections: list[paths.Rejection] = []
    results = document.get("Results")
    for result_index, element in enumerate(
        results if isinstance(results, list) else []
    ):
        counters[trivy.COUNTER_RESULTS] += 1
        if not isinstance(element, dict):
            counters[trivy.COUNTER_RESULTS_SKIPPED_NON_MAPPING] += 1
            continue
        held_a_record = False
        for section in trivy.SUPPORTED_SECTIONS:
            section_records = element.get(section)
            if not isinstance(section_records, list) or not section_records:
                continue
            held_a_record = True
            counters[f"{trivy.COUNTER_RECORDS_PREFIX}{section.lower()}"] += len(
                section_records
            )
            for record_index, record in enumerate(section_records):
                handed = (
                    section_key
                    if (result_index, section, record_index) == designated
                    else section
                )
                outcome = trivy.adapt_record(
                    record,
                    section=handed,
                    target=element.get("Target"),
                    tool=TOOL,
                    root=env.root,
                    tool_base=tool_base,
                    globs=env.globs,
                    tally=tally,
                    counters=counters,
                    packages=element.get("Packages") or (),
                    ecosystem=element.get("Type"),
                    result_index=result_index,
                    record_index=record_index,
                )
                if isinstance(outcome, paths.Rejection):
                    rejections.append(outcome)
                else:
                    rows.append(outcome)
        if not held_a_record:
            counters[trivy.COUNTER_RESULTS_WITHOUT_SUPPORTED_SECTION] += 1
    return Adapted(document, rows, rejections, counters, tally)


def derived(name: str) -> Any:
    """Return a deep copy of one committed fixture, for a variant to be built on.

    Named rather than inlined so that every derivation in this file is visibly a copy.
    A fixture is an input and is never mutated: :func:`load_fixture` re-parses it on
    every call and this returns a copy of that fresh parse, so a variant cannot reach
    the file on disk or another test's document.
    """
    return copy.deepcopy(load_fixture(name))


class TrivyAdapterTestCase(unittest.TestCase):
    """Shared assertions every Trivy test in this module holds a row to.

    The schema invariants are asserted on **every** row of **every** document adapted
    here, not only on the positive fixture's: a row that reached the dataset from a
    negative fixture's surviving records is as much a dataset row as any other.
    """

    maxDiff = None

    def assertRowSchema(self, row: Any, *, where: str) -> None:
        """Assert one row satisfies the twelve-field schema and its two never-absent fields.

        Iterates :data:`normalize.emit.FIELDS` -- the single authored constant -- so no
        thirteenth field, no renamed field and no reordering can pass unnoticed, and
        asserts per field so a failure names the field rather than dumping two dicts.
        """
        self.assertIsInstance(row, dict, msg=f"{where}: a row must be a mapping")
        self.assertEqual(
            list(row),
            list(emit.FIELDS),
            msg=(
                f"{where}: a row carries exactly the twelve fields of emit.FIELDS, in "
                "that order, present-with-None rather than omitted"
            ),
        )
        for field in emit.FIELDS:
            with self.subTest(where=where, field=field):
                value = row[field]
                if field in EXPECTED_OPTIONAL_FIELDS:
                    continue
                self.assertIsNotNone(
                    value,
                    msg=(
                        f"{field} is one of emit.REQUIRED_FIELDS and is never absent; "
                        "absence is permitted only for severity_native, start_line, "
                        "cwe, cve and package_coordinate"
                    ),
                )
        self.assertEqual(row["tool"], TOOL, msg=f"{where}: every row names its tool")
        self.assertIn(
            row["scanner_class"],
            trivy.SCANNER_CLASSES,
            msg=(
                f"{where}: scanner_class is one of the three classes Trivy's sections "
                "map to"
            ),
        )
        self.assertIn(
            row["severity_norm"],
            EXPECTED_SEVERITY_NORM,
            msg=f"{where}: severity_norm is one of the five bands and is never absent",
        )
        self.assertIsInstance(
            row["in_scope"], bool, msg=f"{where}: in_scope is a boolean, not a string"
        )
        start_line = row["start_line"]
        if start_line is not None:
            self.assertIsInstance(
                start_line,
                int,
                msg=f"{where}: start_line is an integer or absent, never a string",
            )
            self.assertNotIsInstance(
                start_line, bool, msg=f"{where}: a boolean is not a line number"
            )
        self.assertRelativePath(row["path"], where=where)

    def assertRelativePath(self, value: Any, *, where: str) -> None:
        """Assert an emitted path is present, relative and expressed against the root.

        No absolute path is ever emitted, *including* for archive members and other
        non-filesystem coordinates (AAP 0.8.2).  ``paths.assert_relative_path`` is the
        emitter's own check and is used here rather than a hand-rolled one, so the two
        cannot drift apart.
        """
        self.assertIsInstance(value, str, msg=f"{where}: path is a string")
        self.assertTrue(value, msg=f"{where}: path is never empty")
        self.assertFalse(
            paths.is_absolute_path(value),
            msg=f"{where}: no emitted path is absolute -- observed {value!r}",
        )
        # Raises PathPolicyError on an absolute path, a drive prefix or a URI scheme.
        paths.assert_relative_path(value)
        self.assertNotIn(
            environment().root,
            value,
            msg=f"{where}: an emitted path never carries the scan root",
        )

    def assertRowsEqualExpected(
        self, rows: list[dict[str, Any]], expected_rows: list[Any], *, where: str
    ) -> None:
        """Assert the emitted rows equal the hand-verified rows, field by field.

        The count first, then every field of every row through
        :data:`normalize.emit.FIELDS` with the row index and field name in the subTest,
        so a failure names exactly which field of which row moved.  Comparing the two
        lists whole would report one opaque mismatch for a single changed character.
        """
        self.assertEqual(
            len(rows),
            len(expected_rows),
            msg=(
                f"{where}: the adapter emitted {len(rows)} row(s) and the hand-verified "
                f"expectation states {len(expected_rows)}"
            ),
        )
        for index, (row, expected_row) in enumerate(zip(rows, expected_rows)):
            self.assertRowSchema(row, where=f"{where} row {index}")
            self.assertEqual(
                list(expected_row),
                list(emit.FIELDS),
                msg=(
                    f"{where} row {index}: the expected row itself carries the twelve "
                    "fields in emit.FIELDS order"
                ),
            )
            for field in emit.FIELDS:
                with self.subTest(where=where, row=index, field=field):
                    self.assertEqual(
                        row[field],
                        expected_row[field],
                        msg=f"{where} row {index}: field {field}",
                    )

    def assertCountersEqualExpected(
        self, adapted: Adapted, expected: Any, *, where: str
    ) -> None:
        """Assert the adapter's counters equal the expected file's, key by key.

        The counter key set is asserted too: it must be exactly
        :data:`trivy.COUNTER_KEYS`, in that order, so a counter added to the adapter
        without an expectation is visible rather than silently unasserted.
        """
        self.assertEqual(
            tuple(adapted.counters),
            trivy.COUNTER_KEYS,
            msg=f"{where}: the counter mapping's keys are exactly trivy.COUNTER_KEYS",
        )
        expected_counters = expected["counters"]
        self.assertEqual(
            sorted(expected_counters),
            sorted(adapted.counters),
            msg=f"{where}: the expected file covers every counter the adapter returns",
        )
        for key in trivy.COUNTER_KEYS:
            with self.subTest(where=where, counter=key):
                self.assertEqual(
                    adapted.counters[key],
                    expected_counters[key],
                    msg=f"{where}: counter {key}",
                )

    def assertOneOutcomePerRecord(self, adapted: Adapted, *, where: str) -> None:
        """Assert every record walked became exactly one row or exactly one rejection.

        The per-artifact half of ``raw finding records = dataset rows + rejected
        records``.  Nothing is dropped and nothing is emitted twice: a filtered record
        would balance no identity, and a duplicated one would be a row the artifact has
        no record for.
        """
        self.assertEqual(
            adapted.raw_records,
            len(adapted.rows) + len(adapted.rejections),
            msg=(
                f"{where}: {adapted.raw_records} record(s) walked must yield "
                f"{len(adapted.rows)} row(s) + {len(adapted.rejections)} rejection(s)"
            ),
        )


class FixtureCorpusTest(TrivyAdapterTestCase):
    """The corpus this module asserts over is present, and its absence is reported.

    Presence is observed rather than assumed.  The precedent provisioning wrote no
    Trivy artifact at all, so a fixture is the only thing this module can assert over
    and a missing one is a blocking gap rather than a test to skip.
    """

    def test_every_fixture_this_module_reads_is_committed(self) -> None:
        """Each required fixture exists, is a file and parses as a JSON object."""
        for name in required_fixture_names():
            with self.subTest(fixture=name):
                location = FIXTURES_DIR / name
                self.assertTrue(
                    location.is_file(),
                    msg=(
                        f"blocking gap: {location} is absent, so the behaviour it "
                        "covers cannot be asserted. Reported, not skipped."
                    ),
                )
                self.assertIsInstance(
                    json.loads(location.read_text(encoding="utf-8")),
                    dict,
                    msg=f"{name}: a Trivy native artifact's top level is an object",
                )

    def test_every_expected_file_this_module_reads_is_committed(self) -> None:
        """Each hand-verified expectation exists and names this adapter."""
        for name in required_expected_names():
            with self.subTest(expected=name):
                location = EXPECTED_DIR / name
                self.assertTrue(
                    location.is_file(),
                    msg=f"blocking gap: {location} is absent. Reported, not skipped.",
                )
                document = json.loads(location.read_text(encoding="utf-8"))
                self.assertEqual(
                    document["adapter"],
                    "harness/lib/normalize/adapters/trivy.py",
                    msg=f"{name}: the expectation names the adapter under test",
                )
                self.assertEqual(
                    document["tool"], TOOL, msg=f"{name}: and the tool under test"
                )

    def test_the_scan_root_is_temporary_empty_and_outside_the_repository(self) -> None:
        """Nothing asserted here can depend on the working checkout or on the run's tree.

        The run's own artifact may or may not exist -- the precedent provisioning wrote
        none -- so a test whose outcome depended on the tree would pass or fail for a
        reason that has nothing to do with the adapter.  The root is a temporary
        directory that exists and holds nothing, which is sufficient because no code
        path under test reads the tree.
        """
        env = environment()
        root = Path(env.root)
        self.assertTrue(root.is_absolute(), msg="the root is absolute, as adapt requires")
        self.assertTrue(root.is_dir(), msg="and it exists")
        self.assertEqual(
            list(root.iterdir()), [], msg="and holds no file: resolution reads no tree"
        )
        self.assertNotEqual(
            root, REPO_ROOT, msg="the root is never the repository being worked in"
        )
        self.assertFalse(
            root.is_relative_to(REPO_ROOT),
            msg="nor anywhere inside it, so no assertion can reach a committed file",
        )

    def test_every_rejection_condition_has_its_own_fixture(self) -> None:
        """The seven conditions this adapter can produce each have one committed fixture.

        The mapping is asserted in both directions: every stem has a class and every
        class named is a literal member of :data:`normalize.paths.REJECT_CLASSES`,
        equal to the module's own constant rather than to a string spelled by hand.
        """
        self.assertEqual(
            sorted(REJECT_CLASS_BY_STEM),
            sorted(REJECT_FIXTURE_STEMS),
            msg="every negative fixture stem is mapped to exactly one class",
        )
        for stem, reject_class in sorted(REJECT_CLASS_BY_STEM.items()):
            with self.subTest(stem=stem):
                self.assertIn(
                    reject_class,
                    paths.REJECT_CLASSES,
                    msg=f"{reject_class!r} is a member of the closed set of ten",
                )
                self.assertTrue(
                    paths.is_reject_class(reject_class),
                    msg="and paths.py itself recognises it",
                )
        self.assertEqual(
            len(set(REJECT_CLASS_BY_STEM.values())),
            len(REJECT_FIXTURE_STEMS),
            msg="no two fixtures assert the same rejection class",
        )


class FixtureIntegrityTest(TrivyAdapterTestCase):
    """Every committed fixture is read and never modified.

    A fixture is the input and the expected file is the assertion; the dependency runs
    one way.  A test that repaired a fixture to make itself pass would have tested the
    fixture rather than the adapter, so the digests taken at module setup are asserted
    again here.
    """

    def test_no_fixture_changed_on_disk_during_this_run(self) -> None:
        """Each fixture's sha256 is what it was when the module was set up."""
        self.assertEqual(
            sorted(_FIXTURE_DIGESTS),
            sorted(required_fixture_names()),
            msg="a digest was taken for every fixture this module reads",
        )
        for name, digest in sorted(_FIXTURE_DIGESTS.items()):
            with self.subTest(fixture=name):
                self.assertEqual(
                    _sha256(FIXTURES_DIR / name),
                    digest,
                    msg=f"{name} was modified during the run; fixtures are read-only",
                )

    def test_loading_a_fixture_twice_yields_independent_documents(self) -> None:
        """Two loads share no object, so a derived variant cannot reach another test."""
        first = load_fixture(POSITIVE_FIXTURE)
        second = load_fixture(POSITIVE_FIXTURE)
        self.assertEqual(first, second, msg="the same file parses to the same document")
        self.assertIsNot(first, second, msg="but not to the same object")
        first["Results"][0]["Target"] = "mutated-in-this-test-only"
        self.assertNotEqual(
            first["Results"][0]["Target"],
            second["Results"][0]["Target"],
            msg="mutating one load leaves the other untouched",
        )
        self.assertEqual(
            load_fixture(POSITIVE_FIXTURE)["Results"][0]["Target"],
            second["Results"][0]["Target"],
            msg="and leaves the file on disk untouched",
        )


class ContractConstantsTest(TrivyAdapterTestCase):
    """The constants the assertions below key on are the modules' own, and agree.

    Every assertion in this file reads its section names, field order, classes and
    rejection vocabulary from a module constant rather than restating them.  These
    tests are what make that worth doing: they establish that the constants say what
    the assertions assume, so a change to one of them fails here with its own message
    instead of surfacing as a puzzling row mismatch twenty tests later.
    """

    def test_the_field_order_is_emits_and_the_adapter_agrees_with_it(self) -> None:
        """``emit.FIELDS`` is the twelve fields in order, and ``trivy.FIELDS`` matches.

        The adapter cannot import ``emit`` -- each adapter depends on ``paths`` and
        ``severity`` and on nothing else (AAP 0.6.4) -- so its copy must agree by
        construction, and this is where that is checked.
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
            msg="the twelve fields, in the request's order (AAP 0.8.2)",
        )
        self.assertEqual(
            trivy.FIELDS,
            emit.FIELDS,
            msg="the adapter's authored copy agrees with emit.py's constant",
        )
        self.assertEqual(
            emit.OPTIONAL_FIELDS,
            EXPECTED_OPTIONAL_FIELDS,
            msg="absence is permitted for exactly five fields",
        )
        self.assertEqual(
            trivy.ABSENCE_PERMITTED_FIELDS,
            EXPECTED_OPTIONAL_FIELDS,
            msg="and the adapter's copy of that set agrees",
        )
        self.assertNotIn(
            "path", emit.OPTIONAL_FIELDS, msg="path is never absent (AAP 0.5.4)"
        )
        self.assertNotIn(
            "severity_norm", emit.OPTIONAL_FIELDS, msg="nor is severity_norm"
        )

    def test_the_three_supported_sections_map_to_the_three_classes(self) -> None:
        """``SUPPORTED_SECTIONS`` is the section-to-class mapping, in walk order."""
        self.assertEqual(
            dict(trivy.SUPPORTED_SECTIONS),
            {
                "Vulnerabilities": "vuln",
                "Secrets": "secret",
                "Misconfigurations": "misconfig",
            },
            msg="AAP 0.5.4's class table for the one tool whose class is per record",
        )
        self.assertEqual(
            tuple(trivy.SUPPORTED_SECTIONS),
            ("Vulnerabilities", "Secrets", "Misconfigurations"),
            msg="the insertion order is the iteration order and therefore the row order",
        )
        self.assertEqual(
            trivy.SCANNER_CLASSES,
            ("vuln", "secret", "misconfig"),
            msg="derived from the mapping so it cannot drift from it",
        )

    def test_the_class_is_resolved_from_a_section_name_and_nothing_else(self) -> None:
        """``scanner_class_for_section`` answers for the three and refuses everything else.

        The resolver is a total function of the section **name**: it takes no record, so
        there is no record content it could consult.  Anything outside the three -- a
        non-finding member, an unsupported finding section's name, a misspelling, a
        non-string -- yields ``None``, which is what becomes an ``unattributable_section``
        rejection rather than a guessed class.
        """
        for section, expected_class in trivy.SUPPORTED_SECTIONS.items():
            with self.subTest(section=section):
                self.assertEqual(
                    trivy.scanner_class_for_section(section), expected_class
                )
        for foreign in (
            SEAM_SECTION_KEY,
            *trivy.UNSUPPORTED_FINDING_SECTIONS,
            "vulnerabilities",
            "Vulnerability",
            "",
            None,
            7,
            ["Vulnerabilities"],
        ):
            with self.subTest(section=foreign):
                self.assertIsNone(
                    trivy.scanner_class_for_section(foreign),
                    msg="a section outside the three establishes no class",
                )

    def test_there_is_no_module_level_scanner_class_constant(self) -> None:
        """The adapter exposes no fixed class, unlike every sibling adapter.

        A single ``SCANNER_CLASS`` constant here would be the very defect the
        section-bound design exists to prevent, so its absence is asserted rather than
        assumed: it is the structural reason a content sniff has nothing to fall back on.
        """
        self.assertFalse(
            hasattr(trivy, "SCANNER_CLASS"),
            msg=(
                "Trivy's class is per record, taken from the section array; a "
                "module-level constant would contradict that by construction"
            ),
        )

    def test_the_unsupported_finding_sections_are_the_two_report_go_declares(self) -> None:
        """``Licenses`` and ``ExperimentalModifiedFindings``, and they are not supported.

        Both are members of Trivy 0.74.0's ``Result`` struct, so both are shapes this
        artifact can legitimately carry -- which is exactly why each is validated empty
        rather than ignored.
        """
        self.assertEqual(
            trivy.UNSUPPORTED_FINDING_SECTIONS,
            ("Licenses", "ExperimentalModifiedFindings"),
            msg="the two finding sections 0.74.0 can emit that this dataset drops",
        )
        for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
            with self.subTest(section=section):
                self.assertNotIn(
                    section,
                    trivy.SUPPORTED_SECTIONS,
                    msg="an unsupported section is not one of the three",
                )
                self.assertIn(
                    section,
                    trivy.RESULT_KNOWN_KEYS,
                    msg="but it is a known Result member, not an unknown key",
                )

    def test_the_rejection_vocabulary_and_the_stop_reasons_are_disjoint(self) -> None:
        """A stop is not a rejection, and no name belongs to both vocabularies.

        The nearest neighbours in this adapter's contract are one line apart: a record
        no section claims is a counted rejection under ``unattributable_section``, while
        a section structure that cannot be read at all stops the run.  A test that
        accepted either for either would have stopped testing the distinction.
        """
        self.assertTrue(
            set(trivy.HALT_REASONS).isdisjoint(paths.REJECT_CLASSES),
            msg="no stop reason is a rejection class, and no rejection class is a reason",
        )
        self.assertIn(
            trivy.HALT_UNSUPPORTED_SECTION,
            trivy.HALT_REASONS,
            msg="the unsupported-section reason is one of the four",
        )
        self.assertNotIn(
            trivy.HALT_UNSUPPORTED_SECTION,
            paths.REJECT_CLASSES,
            msg="and it is not a class anything may be counted under",
        )
        self.assertIn(
            paths.REJECT_UNATTRIBUTABLE_SECTION,
            paths.REJECT_CLASSES,
            msg="while its nearest neighbour is a counted class",
        )
        self.assertTrue(
            issubclass(trivy.UnsupportedTrivySection, Exception),
            msg="the stop is an Exception",
        )
        self.assertFalse(
            issubclass(trivy.UnsupportedTrivySection, trivy.TrivyAdapterError),
            msg=(
                "and deliberately not the module's caller-fault error: a caller "
                "catching argument faults must not be able to swallow it"
            ),
        )

    def test_the_allowlist_loaded_from_this_test_is_the_authoritative_twelve(self) -> None:
        """The twelve globs written here are the twelve ``paths.py`` authors, in order.

        Written independently and read back through the real loader, so the comparison
        is between the request's globs and the module's -- not between the module's copy
        and itself.
        """
        env = environment()
        self.assertEqual(
            env.globs, AUTHORITATIVE_GLOBS, msg="the loader preserved them byte-exact"
        )
        self.assertTrue(
            paths.allowlist_matches_authoritative_globs(env.globs),
            msg="and they are the twelve authoritative globs paths.py states",
        )
        self.assertEqual(len(env.globs), 12, msg="twelve, neither widened nor narrowed")
        self.assertNotIn(
            paths.SRC_TEST_MARKER,
            "".join(env.globs),
            msg="the allowlist carries no exclusion line; the exclusion is paths.py's",
        )

    def test_the_recorded_path_bases_are_the_two_kinds_the_metadata_declares(self) -> None:
        """Both readings load through ``paths.py`` and behave as their kind requires."""
        env = environment()
        merged = env.tool_base(BASE_KIND_MERGED)
        part = env.tool_base(BASE_KIND_PART)
        self.assertEqual(merged.tool, TOOL)
        self.assertEqual(merged.kind, paths.PATH_BASE_KIND_SCAN_ROOT)
        self.assertEqual(
            merged.base_for_relative(),
            env.root,
            msg="the merged artifact's Targets anchor on the scan root",
        )
        self.assertTrue(merged.has_explicit_base)
        self.assertEqual(part.kind, paths.PATH_BASE_KIND_PER_SECTION_TARGET)
        self.assertIsNone(
            part.base_for_relative(),
            msg=(
                "a per-directory part supplies no single base, which is why a relative "
                "per-record path under it is a counted rejection rather than a silent "
                "reading against the root"
            ),
        )
        self.assertEqual(
            part.scan_root, env.root, msg="the root is still recorded for the part"
        )


class PositiveMappingTest(TrivyAdapterTestCase):
    """The positive fixture maps field for field onto its hand-verified expectation.

    The fixture is a Trivy 0.74.0 filesystem report carrying all three supported
    finding sections, and the expectation was derived by reading it and the authored
    contracts rather than by recording what the adapter printed.  Where the two
    disagree, the disagreement is the finding: it is diagnosed, never papered over by
    editing either file.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the positive fixture once for the whole class."""
        cls.adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))
        cls.expected = load_expected("trivy")

    def test_the_row_count_is_the_expected_files_exactly(self) -> None:
        """Assertion 1: neither a row more nor a row fewer."""
        self.assertEqual(
            len(self.adapted.rows),
            len(self.expected["rows"]),
            msg="the emitted row count equals the hand-verified count",
        )
        self.assertEqual(
            len(self.adapted.rows),
            self.expected["counts"]["rows"],
            msg="and the count the expectation states in its own counts block",
        )
        self.assertEqual(
            self.adapted.rejections,
            [],
            msg="the positive fixture rejects nothing: every record becomes a row",
        )

    def test_every_row_matches_field_by_field_over_the_twelve_fields(self) -> None:
        """Assertions 2 and 3: every field of every row, iterating ``emit.FIELDS``."""
        self.assertRowsEqualExpected(
            self.adapted.rows, self.expected["rows"], where=POSITIVE_FIXTURE
        )
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                self.assertEqual(
                    row["tool"], TOOL, msg="assertion 3: tool is trivy on every row"
                )

    def test_the_rows_satisfy_the_schema_the_emitter_enforces(self) -> None:
        """Every row survives ``emit.validate_rows`` unchanged and in order.

        The emitter is the last line of defence on the two never-absent fields, so
        putting the rows through it here proves the adapter's output is writable as a
        dataset rather than merely equal to a list of dicts.
        """
        validated = emit.validate_rows(self.adapted.rows)
        self.assertEqual(
            validated,
            self.adapted.rows,
            msg="validation neither reorders, drops nor rewrites a row",
        )

    def test_one_outcome_per_record_walked(self) -> None:
        """Nine records walked, nine rows, nothing dropped and nothing duplicated."""
        self.assertOneOutcomePerRecord(self.adapted, where=POSITIVE_FIXTURE)
        self.assertEqual(
            self.adapted.raw_records,
            self.expected["counts"]["raw_finding_records"],
            msg="the record count matches the expectation's own count unit",
        )
        self.assertEqual(
            {
                section: sum(
                    1 for record in self.adapted.records if record.section == section
                )
                for section in trivy.SUPPORTED_SECTIONS
            },
            self.expected["fixture"]["records_by_section"],
            msg="and the per-section split is the expectation's",
        )

    def test_the_counters_are_the_expected_files(self) -> None:
        """Every counter, including the four AAP 0.5.4 has reported per tool."""
        self.assertCountersEqualExpected(
            self.adapted, self.expected, where=POSITIVE_FIXTURE
        )
        reported = self.expected["aap_reported_counters"]
        for key in (
            trivy.COUNTER_MULTI_LOCATION,
            trivy.COUNTER_MULTI_VALUED_CWE,
            trivy.COUNTER_MULTI_VALUED_CVE,
            trivy.COUNTER_NON_FILESYSTEM_PATHS,
        ):
            with self.subTest(counter=key):
                self.assertEqual(self.adapted.counters[key], reported[key])

    def test_no_emitted_path_is_absolute_and_every_path_is_present(self) -> None:
        """Assertion 7, on every row: relative to the root, never absolute, never absent."""
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                self.assertRelativePath(row["path"], where=f"row {index}")
        for index, expected_row in enumerate(self.expected["rows"]):
            with self.subTest(expected_row=index):
                self.assertFalse(
                    paths.is_absolute_path(expected_row["path"]),
                    msg="no expected value in this file is an absolute path either",
                )

    def test_the_path_comes_from_the_enclosing_target_refined_where_the_section_supplies_one(
        self,
    ) -> None:
        """The Target is the base; only ``Vulnerabilities`` supplies a per-record path.

        ``DetectedSecret`` and ``DetectedMisconfiguration`` declare no path field, so a
        secret and a misconfiguration take their Target unrefined -- and a
        misconfiguration's ``CauseMetadata.Resource`` names a resource rather than a
        file, so reading one as a path would be inference.  Exactly one record in this
        fixture refines its Target, through ``PkgPath``.
        """
        refinements = 0
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                declared = record.record.get("PkgPath")
                if record.section != "Vulnerabilities":
                    self.assertIsNone(
                        declared,
                        msg=(
                            "a secret and a misconfiguration declare no path field, so "
                            "neither can refine its Target"
                        ),
                    )
                if isinstance(declared, str) and declared:
                    refinements += 1
                    self.assertEqual(
                        row["path"],
                        declared,
                        msg="a per-record path refines the Target",
                    )
                else:
                    self.assertEqual(
                        row["path"],
                        record.target,
                        msg="and without one the Target is the whole coordinate",
                    )
        self.assertEqual(
            refinements,
            self.adapted.counters[trivy.COUNTER_PER_RECORD_PATH_REFINEMENTS],
            msg="and the adapter counted the refinements this walk found",
        )
        self.assertEqual(
            refinements, 1, msg="exactly one record in this fixture refines its Target"
        )


#: A record carrying every one of the three sections' rule-identifier fields at once,
#: plus a package coordinate so the dependency-oriented section cannot reject it, plus
#: both line members so a line is available whichever section reads it.
#:
#: Its purpose is to be handed to the adapter three times, byte-identical, under three
#: different sections. Identical content that yields three different classes is the
#: strongest available statement that the class comes from the section: no sniff of any
#: kind can distinguish three calls whose record is the same object's copy.
AMBIGUOUS_RECORD: dict[str, Any] = {
    "VulnerabilityID": "CVE-2021-3749",
    "RuleID": "jwt-token",
    "ID": "DS-0031",
    "Title": "a record whose content names all three sections' identifier fields",
    "Severity": "HIGH",
    "StartLine": 11,
    "CauseMetadata": {"StartLine": 22},
    "PkgName": "example",
    "InstalledVersion": "1.0.0",
    "PkgIdentifier": {"PURL": "pkg:npm/example@1.0.0"},
}

#: The rule-identifier field each section reads, and therefore the identifier the record
#: above yields under each. Restated from AAP 0.5.4's trivy row so the per-section choice
#: is asserted rather than assumed to follow from the class.
RULE_ID_FIELD_BY_SECTION = {
    "Vulnerabilities": "VulnerabilityID",
    "Secrets": "RuleID",
    "Misconfigurations": "ID",
}


class ScannerClassFromSectionTest(TrivyAdapterTestCase):
    """``scanner_class`` derives from the section array, never from record content.

    Trivy is the single tool in AAP 0.5.4's class table whose class is not fixed, and a
    content sniff is the failure this adapter's whole shape exists to prevent: a
    ``Result`` element can carry three finding arrays at once, and a secret and a
    misconfiguration have overlapping-looking fields, so a sniff yields
    plausible-but-wrong classes that **no reconciliation check would catch**.  Every
    row's class is therefore asserted against the array the record sits in, read
    straight out of the document.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the positive fixture once, and hold its section walk."""
        cls.adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))

    def test_every_rows_class_is_the_class_its_enclosing_array_dictates(self) -> None:
        """Assertion 4, per row, with the section names read from the module constant."""
        self.assertEqual(
            len(self.adapted.rows),
            len(self.adapted.records),
            msg="the positive fixture rejects nothing, so rows and records align",
        )
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer, section=record.section):
                self.assertEqual(
                    row["scanner_class"],
                    record.expected_scanner_class,
                    msg=(
                        f"the record at {record.pointer} was read from "
                        f"{record.section}[], which trivy.SUPPORTED_SECTIONS maps to "
                        f"{record.expected_scanner_class!r}"
                    ),
                )
                self.assertEqual(
                    row["scanner_class"],
                    trivy.scanner_class_for_section(record.section),
                    msg="and the module's own resolver agrees on the section's class",
                )

    def test_the_class_totals_are_the_section_totals(self) -> None:
        """Each class carries exactly as many rows as its section held records."""
        for section, klass in trivy.SUPPORTED_SECTIONS.items():
            with self.subTest(section=section):
                from_records = sum(
                    1 for record in self.adapted.records if record.section == section
                )
                from_rows = sum(
                    1 for row in self.adapted.rows if row["scanner_class"] == klass
                )
                self.assertEqual(from_records, from_rows)
                self.assertEqual(
                    from_rows,
                    self.adapted.counters[f"{trivy.COUNTER_ROWS_CLASS_PREFIX}{klass}"],
                    msg="and the adapter's own per-class counter agrees",
                )
        self.assertEqual(
            {row["scanner_class"] for row in self.adapted.rows},
            set(trivy.SCANNER_CLASSES),
            msg="all three classes appear, so no section is silently unexercised",
        )

    def test_the_record_whose_content_names_the_wrong_section_still_follows_its_array(
        self,
    ) -> None:
        """Assertion 5: the falsifying record, without which assertion 4 proves nothing.

        ``/Results/0/Misconfigurations/0`` is a Dockerfile misconfiguration whose every
        piece of prose points at the secret scanner: its ``Title`` is about secrets
        passed via build-args, its ``Message`` names an exposed environment variable and
        its ``Resolution`` talks about secret mounts.  Any implementation that keyed the
        class off record content -- a search for a word, a guess from the identifier, a
        try-all-three read of ``VulnerabilityID`` then ``RuleID`` then ``ID`` -- would
        class this row ``secret`` and still satisfy every other assertion in this file,
        because no other record's prose disagrees with its array.

        The misleading content is asserted to be present first.  A falsifier that stopped
        misleading would silently stop falsifying.
        """
        record = self.adapted.records[0]
        self.assertEqual(record.pointer, "/Results/0/Misconfigurations/0")
        self.assertEqual(record.section, "Misconfigurations")
        prose = " ".join(
            str(record.record.get(key, ""))
            for key in ("Title", "Message", "Resolution")
        ).lower()
        self.assertIn(
            "secret",
            prose,
            msg=(
                "the falsifier only falsifies while its content points at another "
                "section; this record's own prose must still name secrets"
            ),
        )
        self.assertEqual(
            self.adapted.rows[0]["scanner_class"],
            "misconfig",
            msg=(
                "it is an element of Results[0].Misconfigurations[], and the section "
                "array is the sole source of the class. If this row ever reads secret, "
                "the derivation has been inverted"
            ),
        )

    def test_the_corollary_runs_the_other_way_on_the_secret_whose_identifier_is_a_format(
        self,
    ) -> None:
        """A ``jwt-token`` record is a secret because of its array, not its identifier."""
        record = next(
            item for item in self.adapted.records if item.section == "Secrets"
        )
        self.assertEqual(record.record["RuleID"], "jwt-token")
        row = self.adapted.rows[self.adapted.records.index(record)]
        self.assertEqual(
            row["scanner_class"],
            "secret",
            msg="from Secrets[], not from an identifier that names a token format",
        )
        self.assertEqual(row["rule_id"], "jwt-token")

    def test_one_identical_record_takes_three_classes_under_three_sections(self) -> None:
        """The same content, three sections, three classes -- and three identifiers.

        Handed through :func:`trivy.adapt_record`, the public section-attribution seam.
        Nothing about the record changes between the three calls, so any implementation
        that consulted content would have to return the same class three times.  The
        rule identifier changing with the section is asserted alongside, because it is
        the same principle applied to a different field: the field is chosen by section
        (AAP 0.5.4), not by whichever identifier happens to be present.
        """
        env = environment()
        for section, klass in trivy.SUPPORTED_SECTIONS.items():
            with self.subTest(section=section):
                counters = trivy.new_counters()
                outcome = trivy.adapt_record(
                    copy.deepcopy(AMBIGUOUS_RECORD),
                    section=section,
                    target="core/src/main/scala/org/apache/spark/SparkContext.scala",
                    tool=TOOL,
                    root=env.root,
                    tool_base=env.tool_base(),
                    globs=env.globs,
                    tally=severity.LiteralTally.with_all_tools(),
                    counters=counters,
                    ecosystem="npm",
                )
                self.assertNotIsInstance(
                    outcome,
                    paths.Rejection,
                    msg="the record is complete under every section, so it is a row",
                )
                self.assertEqual(
                    outcome["scanner_class"],
                    klass,
                    msg=f"{section}[] maps to {klass!r} and content decides nothing",
                )
                self.assertEqual(
                    outcome["rule_id"],
                    AMBIGUOUS_RECORD[RULE_ID_FIELD_BY_SECTION[section]],
                    msg=(
                        "and the rule identifier is read from the field this section "
                        f"declares, {RULE_ID_FIELD_BY_SECTION[section]}"
                    ),
                )

    def test_a_content_hint_planted_in_a_derived_document_changes_no_class(self) -> None:
        """A CVE in a misconfiguration's title, and a ``RuleID`` on a vulnerability.

        The two shapes an implementation is most likely to sniff, planted in a derived
        copy of the fixture rather than in the fixture itself.  Both rows keep the class
        their array dictates.
        """
        document = derived(POSITIVE_FIXTURE)
        misconfiguration = document["Results"][0]["Misconfigurations"][0]
        misconfiguration["Title"] = "CVE-2021-3749 mentioned inside a misconfiguration"
        vulnerability = document["Results"][3]["Vulnerabilities"][0]
        vulnerability["RuleID"] = "generic-api-key"
        vulnerability["Match"] = "****"
        adapted = adapt_document(document)
        rows_by_pointer = {
            record.pointer: row for row, record in zip(adapted.rows, adapted.records)
        }
        self.assertEqual(
            rows_by_pointer["/Results/0/Misconfigurations/0"]["scanner_class"],
            "misconfig",
            msg="a CVE in the prose does not make a misconfiguration a vulnerability",
        )
        self.assertEqual(
            rows_by_pointer["/Results/0/Misconfigurations/0"]["message"],
            misconfiguration["Title"],
            msg="the planted title did reach the message, so the record was read",
        )
        self.assertEqual(
            rows_by_pointer["/Results/3/Vulnerabilities/0"]["scanner_class"],
            "vuln",
            msg="and a RuleID-looking field does not make a vulnerability a secret",
        )
        self.assertEqual(
            rows_by_pointer["/Results/3/Vulnerabilities/0"]["rule_id"],
            vulnerability["VulnerabilityID"],
            msg="the identifier still comes from the field this section declares",
        )


class StartLineSectionDependenceTest(TrivyAdapterTestCase):
    """``start_line`` is section-dependent, and its absence is stated rather than filled.

    ``report.go`` puts ``StartLine`` on a secret and inside a misconfiguration's
    ``CauseMetadata``, and declares none at all on a vulnerability.  So one document
    proves both halves without a single synthesized absence -- and the absent half is
    asserted explicitly, because an implementation that defaulted to ``0`` or ``1``
    would satisfy a mere "is an integer or None" check on every row that has a line.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the positive fixture once for the whole class."""
        cls.adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))

    def test_a_vulnerability_row_carries_no_line_at_all(self) -> None:
        """Assertion 6, the half that catches a default: ``None``, not ``0`` and not ``1``."""
        vulnerability_rows = [
            (row, record)
            for row, record in zip(self.adapted.rows, self.adapted.records)
            if record.section == "Vulnerabilities"
        ]
        self.assertTrue(
            vulnerability_rows, msg="the fixture carries vulnerability records to assert on"
        )
        for row, record in vulnerability_rows:
            with self.subTest(pointer=record.pointer):
                self.assertIsNone(
                    row["start_line"],
                    msg=(
                        "report.go declares no line member on a DetectedVulnerability, "
                        "so the field is absent -- not zero, not one, and not the "
                        "enclosing package's location"
                    ),
                )
                self.assertNotIn(
                    "StartLine",
                    record.record,
                    msg="and the record itself states none, which is why",
                )

    def test_a_secret_and_a_misconfiguration_row_carry_the_line_they_state(self) -> None:
        """The present half, read from the member each section declares."""
        seen = set()
        for row, record in zip(self.adapted.rows, self.adapted.records):
            if record.section == "Vulnerabilities":
                continue
            with self.subTest(pointer=record.pointer):
                if record.section == "Secrets":
                    stated = record.record.get("StartLine")
                else:
                    stated = (record.record.get("CauseMetadata") or {}).get("StartLine")
                self.assertIsInstance(
                    stated, int, msg="the fixture states a line for this record"
                )
                self.assertEqual(
                    row["start_line"],
                    stated,
                    msg="and the row carries that line, unmodified",
                )
                seen.add(record.section)
        self.assertEqual(
            seen,
            {"Secrets", "Misconfigurations"},
            msg="both line-bearing sections are exercised, not just one",
        )

    def test_the_absence_count_is_the_number_of_vulnerability_records(self) -> None:
        """The adapter counted the absences this walk found, rather than assuming them."""
        absences = sum(
            1 for row in self.adapted.rows if row["start_line"] is None
        )
        self.assertEqual(
            absences,
            self.adapted.counters[trivy.COUNTER_START_LINE_ABSENT],
            msg="rows with no line and the counter agree",
        )
        self.assertEqual(
            absences,
            sum(1 for record in self.adapted.records if record.section == "Vulnerabilities"),
            msg="and every one of them is a vulnerability record",
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_START_LINE_ZERO],
            0,
            msg="no record here states a zero line, so none was read as an absence",
        )

    def test_a_stated_zero_line_is_read_as_an_absence_and_not_as_line_zero(self) -> None:
        """A derived secret stating ``StartLine`` 0 yields a row with no line.

        Zero is not a line number, and the artifact stating one is a stated absence
        rather than a fault: the record is emitted with ``start_line`` absent and the
        reading is counted, so it is visible rather than silently rounded into 1 or
        rejected.
        """
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][1])]
        document["Results"][0]["Secrets"] = [
            copy.deepcopy(document["Results"][0]["Secrets"][0])
        ]
        document["Results"][0]["Secrets"][0]["StartLine"] = 0
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rejections, [], msg="a stated zero is not a rejection")
        self.assertIsNone(
            adapted.rows[0]["start_line"], msg="and it is not line zero either"
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_START_LINE_ZERO],
            1,
            msg="the reading is counted, so it is never silent",
        )


class SeverityMappingTest(TrivyAdapterTestCase):
    """``severity_native`` is the literal as observed; ``severity_norm`` is policy.

    The policy is fixed before any output is observed (AAP 0.5.4): a native label in the
    mapped vocabulary governs, a CVSS score is consulted only where no mapped label
    exists, and a literal outside every mapped vocabulary maps to ``Info`` and is
    **listed with the rows it affected**.  ``severity_norm`` is never absent.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the positive fixture once for the whole class."""
        cls.adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))
        cls.expected = load_expected("trivy")

    def test_the_label_vocabulary_is_the_one_the_policy_states(self) -> None:
        """Ten literals, five bands, restated here independently of ``severity.py``."""
        self.assertEqual(
            severity.label_table(),
            EXPECTED_LABEL_BANDS,
            msg="the label table is the written policy, neither wider nor narrower",
        )
        self.assertEqual(
            severity.SEVERITY_NORM,
            EXPECTED_SEVERITY_NORM,
            msg="and the five bands are the standard scale with None emitted as Info",
        )

    def test_every_row_keeps_the_literal_and_takes_the_band_the_map_gives_it(self) -> None:
        """Assertion 8: the raw ``Severity`` literal, and its band by the label map."""
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                stated = record.record.get("Severity")
                self.assertEqual(
                    row["severity_native"],
                    stated,
                    msg="severity_native is the literal as observed, not upper-cased",
                )
                self.assertEqual(
                    row["severity_norm"],
                    EXPECTED_LABEL_BANDS[str(stated).upper()],
                    msg="and the band is the label map's, matched case-insensitively",
                )
                self.assertIsNotNone(
                    row["severity_norm"], msg="severity_norm is never absent"
                )

    def test_the_label_governs_even_where_the_record_also_carries_scores(self) -> None:
        """Label over score: four records here state both, and the label decides all four.

        The score tables are present and counted -- so the test is not passing merely
        because nothing was there to consult -- and no row's band came from one.
        """
        with_scores = [
            record for record in self.adapted.records if record.record.get("CVSS")
        ]
        self.assertEqual(
            len(with_scores),
            self.adapted.counters[trivy.COUNTER_SEVERITY_CVSS_ENTRIES_PRESENT],
            msg="the records carrying a score table are counted",
        )
        self.assertTrue(with_scores, msg="and there is at least one to assert over")
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_SEVERITY_BASIS_PREFIX + "cvss_score"],
            0,
            msg="yet no band was taken from a score, because every label is mapped",
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_SEVERITY_BASIS_PREFIX + "label"],
            len(self.adapted.rows),
            msg="every row's band came from its label",
        )

    def test_the_tally_records_every_literal_with_the_rows_it_affected(self) -> None:
        """The tally is ``severity-map.md``'s input, and it is fed once per emitted row."""
        entries = self.adapted.tally.entries(TOOL)
        self.assertEqual(
            sum(entry.rows for entry in entries),
            len(self.adapted.rows),
            msg="one tally record per emitted row, no more and no fewer",
        )
        counted: dict[str, int] = {}
        for row in self.adapted.rows:
            literal = str(row["severity_native"])
            counted[literal] = counted.get(literal, 0) + 1
        self.assertEqual(
            {entry.severity_native: entry.rows for entry in entries},
            counted,
            msg="and each literal's row count is the number of rows carrying it",
        )
        self.assertEqual(
            [entry for entry in entries if entry.unmapped],
            [],
            msg=(
                "no literal in this fixture is unmapped: UNKNOWN looks unmapped and is "
                "in fact in the vocabulary, which is why the unmapped case is asserted "
                "on a derived document instead"
            ),
        )

    def test_a_literal_outside_the_vocabulary_maps_to_info_and_is_disclosed(self) -> None:
        """Assertion 8's second half, on a derived document.

        The positive fixture carries no unmapped literal -- its ``UNKNOWN`` is mapped --
        so the case is exercised on a copy.  An unmapped literal is banded ``Info``,
        never dropped and never guessed at, and it is recorded in the tally **as
        unmapped with its row count**, which is what AAP 0.5.4 requires
        ``severity-map.md`` to list.
        """
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][0])]
        unmapped_literal = "SEVERE"
        self.assertNotIn(
            unmapped_literal,
            severity.label_table(),
            msg="the literal is genuinely outside the mapped vocabulary",
        )
        for record in document["Results"][0]["Misconfigurations"]:
            record["Severity"] = unmapped_literal
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 2)
        for index, row in enumerate(adapted.rows):
            with self.subTest(row=index):
                self.assertEqual(
                    row["severity_native"],
                    unmapped_literal,
                    msg="the literal is retained exactly as observed",
                )
                self.assertEqual(
                    row["severity_norm"],
                    "Info",
                    msg="and an unmapped literal is banded Info by policy",
                )
        unmapped_entries = [
            entry for entry in adapted.tally.entries(TOOL) if entry.unmapped
        ]
        self.assertEqual(
            len(unmapped_entries), 1, msg="one unmapped literal, one tally entry"
        )
        self.assertEqual(unmapped_entries[0].severity_native, unmapped_literal)
        self.assertEqual(unmapped_entries[0].severity_norm, "Info")
        self.assertEqual(
            unmapped_entries[0].basis,
            severity.BASIS_UNMAPPED_LITERAL,
            msg="recorded under the unmapped basis, so severity-map.md can list it",
        )
        self.assertEqual(
            unmapped_entries[0].rows,
            2,
            msg="with the number of rows it affected, not merely that it occurred",
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_SEVERITY_BASIS_PREFIX + "unmapped_literal"],
            2,
            msg="and the adapter counted both rows under that basis",
        )

    def test_the_label_map_is_case_insensitive(self) -> None:
        """``medium``, ``Moderate`` and ``critical`` band as their upper-case forms do."""
        for literal, band in (
            ("medium", "Medium"),
            ("Moderate", "Medium"),
            ("critical", "Critical"),
            ("negligible", "Info"),
            ("Informational", "Info"),
        ):
            with self.subTest(literal=literal):
                document = derived(POSITIVE_FIXTURE)
                document["Results"] = [copy.deepcopy(document["Results"][0])]
                document["Results"][0]["Misconfigurations"] = [
                    copy.deepcopy(document["Results"][0]["Misconfigurations"][0])
                ]
                document["Results"][0]["MisconfSummary"] = {
                    "Successes": 24,
                    "Failures": 1,
                }
                document["Results"][0]["Misconfigurations"][0]["Severity"] = literal
                adapted = adapt_document(document)
                self.assertEqual(len(adapted.rows), 1)
                self.assertEqual(
                    adapted.rows[0]["severity_native"],
                    literal,
                    msg="the literal is not normalised into the row",
                )
                self.assertEqual(
                    adapted.rows[0]["severity_norm"],
                    band,
                    msg="but the lookup that banded it was case-insensitive",
                )

    def test_a_score_governs_only_where_no_mapped_label_exists(self) -> None:
        """With the label removed, the band comes from the score and the score is recorded.

        The rendered score becomes ``severity_native``, so the dataset states what was
        used rather than leaving a reader to guess which of the record's several score
        entries decided the band.
        """
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][2])]
        document["Results"][0]["Vulnerabilities"] = [
            copy.deepcopy(document["Results"][0]["Vulnerabilities"][0])
        ]
        record = document["Results"][0]["Vulnerabilities"][0]
        del record["Severity"]
        self.assertIn("CVSS", record, msg="the record still carries its score table")
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        row = adapted.rows[0]
        self.assertEqual(
            row["severity_native"],
            "7.5",
            msg="the score entry used is rendered into the field, to one decimal",
        )
        self.assertEqual(
            row["severity_norm"], "High", msg="7.5 falls in the 7.0-8.9 band"
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_SEVERITY_BASIS_PREFIX + "cvss_score"],
            1,
            msg="and the basis is recorded as the score rather than a label",
        )
        entry = adapted.tally.entries(TOOL)[0]
        self.assertEqual(entry.basis, severity.BASIS_CVSS_SCORE)
        self.assertFalse(entry.unmapped, msg="a banded score is not an unmapped literal")

    def test_a_record_with_no_vocabulary_at_all_states_the_absence(self) -> None:
        """No label and no score: ``severity_native`` absent, ``severity_norm`` still Info.

        The absence is stated rather than a level being assumed, which is the whole
        distinction between this case and the unmapped one above.
        """
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][1])]
        document["Results"][0]["Secrets"] = [
            copy.deepcopy(document["Results"][0]["Secrets"][0])
        ]
        del document["Results"][0]["Secrets"][0]["Severity"]
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertIsNone(
            adapted.rows[0]["severity_native"],
            msg="absence is permitted for severity_native and is not filled in",
        )
        self.assertEqual(
            adapted.rows[0]["severity_norm"],
            "Info",
            msg="severity_norm is never absent, so the band is Info by policy",
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_SEVERITY_ABSENT],
            1,
            msg="and the absence is counted rather than inferred from a null",
        )


class IdentifierSelectionTest(TrivyAdapterTestCase):
    """``cwe`` and ``cve`` carry one value each, chosen by ascending numeric identifier.

    The ordering is over the integer after the prefix -- year then sequence for a CVE --
    which is total, so no tie arises and no producer-order tiebreak is needed.  The
    number of records carrying more than one value is reported per tool rather than
    lost.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the positive fixture once for the whole class."""
        cls.adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))

    def test_the_multi_valued_record_takes_the_smallest_numeric_identifier(self) -> None:
        """Assertion 9, on the fixture's own record whose two orders disagree.

        Its ``CweIDs`` are ``CWE-1333`` then ``CWE-400``.  Lexicographically the first
        is smaller; numerically the second is, and the second is what the row carries.
        An implementation that sorted the strings, or that took the producer's first
        entry, would emit ``CWE-1333`` here.
        """
        multi_valued = [
            (row, record)
            for row, record in zip(self.adapted.rows, self.adapted.records)
            if isinstance(record.record.get("CweIDs"), list)
            and len(record.record["CweIDs"]) > 1
        ]
        self.assertEqual(
            len(multi_valued),
            1,
            msg="the fixture carries exactly one multi-valued CweIDs record",
        )
        row, record = multi_valued[0]
        identifiers = record.record["CweIDs"]
        self.assertEqual(identifiers, ["CWE-1333", "CWE-400"])
        self.assertEqual(
            min(identifiers),
            "CWE-1333",
            msg="lexicographic order would choose the other one",
        )
        self.assertEqual(
            row["cwe"],
            "CWE-400",
            msg="ascending numeric order chooses 400 over 1333",
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_MULTI_VALUED_CWE],
            1,
            msg="and the multi-valued record is counted, so the choice is visible",
        )

    def test_a_single_valued_record_carries_that_value_and_counts_as_single(self) -> None:
        """One identifier in, the same identifier out; absence where the record states none."""
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                identifiers = record.record.get("CweIDs")
                if not identifiers:
                    self.assertIsNone(
                        row["cwe"], msg="no CweIDs member means no cwe is invented"
                    )
                elif len(identifiers) == 1:
                    self.assertEqual(row["cwe"], identifiers[0])
                else:
                    self.assertIn(row["cwe"], identifiers)

    def test_three_identifiers_out_of_order_still_yield_the_smallest(self) -> None:
        """A derived record whose three values put the numeric minimum last but one."""
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][3])]
        document["Results"][0]["Vulnerabilities"] = [
            copy.deepcopy(document["Results"][0]["Vulnerabilities"][0])
        ]
        identifiers = ["CWE-1004", "CWE-79", "CWE-200"]
        document["Results"][0]["Vulnerabilities"][0]["CweIDs"] = identifiers
        adapted = adapt_document(document)
        self.assertEqual(
            min(identifiers),
            "CWE-1004",
            msg="lexicographic order would choose 1004",
        )
        self.assertEqual(
            adapted.rows[0]["cwe"], "CWE-79", msg="and numeric order chooses 79"
        )
        self.assertEqual(adapted.counters[trivy.COUNTER_MULTI_VALUED_CWE], 1)

    def test_the_cve_field_is_the_vulnerability_id_where_it_is_cve_shaped(self) -> None:
        """``cve`` comes from ``VulnerabilityID`` alone, so it holds one value or none."""
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                identifier = record.record.get("VulnerabilityID")
                if isinstance(identifier, str) and identifier.startswith("CVE-"):
                    self.assertEqual(
                        row["cve"],
                        identifier,
                        msg="a CVE-shaped identifier reaches the cve field",
                    )
                    self.assertEqual(
                        row["rule_id"],
                        identifier,
                        msg="and remains the row's rule identifier as well",
                    )
                else:
                    self.assertIsNone(
                        row["cve"],
                        msg=(
                            "a non-CVE identifier is not coerced into the cve field: "
                            "NSWG-ECO-519 is a rule identifier, not a CVE"
                        ),
                    )

    def test_no_record_can_carry_two_cves_so_the_counter_is_zero(self) -> None:
        """The multi-valued CVE counter is 0 here, and the reason is structural.

        ``cve`` is collected from ``VulnerabilityID``, which holds exactly one
        identifier, so no Trivy record can carry two.  The counter is asserted at zero
        together with that reason, rather than left as an unexplained zero a reader
        would have to take on trust.
        """
        self.assertEqual(self.adapted.counters[trivy.COUNTER_MULTI_VALUED_CVE], 0)
        for record in self.adapted.records:
            with self.subTest(pointer=record.pointer):
                identifier = record.record.get("VulnerabilityID")
                self.assertNotIsInstance(
                    identifier,
                    list,
                    msg="the member is a single identifier, never an array",
                )


class PackageCoordinateTest(TrivyAdapterTestCase):
    """One canonical package coordinate, by a defined candidate precedence.

    A package URL on the record; failing that one on the enclosing package object;
    failing that ``<ecosystem>:<name>@<version>`` from the record's own fields; failing
    that the same from the enclosing package's.  Where none can be formed the field is
    absent -- and for a dependency-oriented record that absence is a rejection with its
    own fixture, which :class:`RejectedRecordTest` asserts.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the positive fixture once for the whole class."""
        cls.adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))

    def test_a_record_purl_is_taken_verbatim(self) -> None:
        """Level 1: the record's own ``PkgIdentifier.PURL``, unmodified."""
        seen = 0
        for row, record in zip(self.adapted.rows, self.adapted.records):
            purl = (record.record.get("PkgIdentifier") or {}).get("PURL")
            if not purl:
                continue
            seen += 1
            with self.subTest(pointer=record.pointer):
                self.assertEqual(row["package_coordinate"], purl)
        self.assertEqual(
            seen,
            self.adapted.counters[trivy.COUNTER_COORDINATE_RECORD_PURL],
            msg="and the level is counted, so the route each row took is visible",
        )
        self.assertGreater(seen, 0, msg="the fixture exercises this level")

    def test_an_enclosing_package_purl_is_the_second_candidate(self) -> None:
        """Level 2: the record states no PURL, and its ``PkgID`` matches a ``Packages`` entry.

        This is ahead of the ecosystem coordinate, so a record with a matching inventory
        entry takes that entry's PURL rather than a composed string.
        """
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_COORDINATE_PACKAGE_PURL],
            1,
            msg="exactly one row in this fixture resolves at level 2",
        )
        def inventory_of(record: Record) -> dict[str, Any]:
            """The enclosing element's package inventory, keyed by its ``ID``."""
            return {
                entry["ID"]: entry
                for entry in (record.element.get("Packages") or [])
                if isinstance(entry, dict) and entry.get("ID")
            }

        matched = [
            (row, record)
            for row, record in zip(self.adapted.rows, self.adapted.records)
            if record.record.get("PkgID")
            and not (record.record.get("PkgIdentifier") or {}).get("PURL")
            and (inventory_of(record).get(record.record["PkgID"]) or {}).get(
                "Identifier", {}
            ).get("PURL")
        ]
        self.assertEqual(
            len(matched),
            1,
            msg=(
                "exactly one record states no PURL of its own while its PkgID matches "
                "an inventory entry that states one"
            ),
        )
        row, record = matched[0]
        entry = inventory_of(record)[record.record["PkgID"]]
        self.assertEqual(
            row["package_coordinate"],
            entry["Identifier"]["PURL"],
            msg="the enclosing package's PURL, not a composed coordinate",
        )

    def test_the_ecosystem_coordinate_is_composed_with_a_lower_cased_ecosystem(self) -> None:
        """Level 3: ``<ecosystem>:<name>@<version>`` from the record's own fields."""
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_COORDINATE_RECORD_FIELDS],
            1,
            msg="exactly one row in this fixture resolves at level 3",
        )
        composed = [
            (row, record)
            for row, record in zip(self.adapted.rows, self.adapted.records)
            if row["package_coordinate"]
            and not str(row["package_coordinate"]).startswith("pkg:")
        ]
        self.assertEqual(len(composed), 1)
        row, record = composed[0]
        self.assertEqual(
            row["package_coordinate"],
            "{}:{}@{}".format(
                str(record.element["Type"]).lower(),
                record.record["PkgName"],
                record.record["InstalledVersion"],
            ),
            msg="the enclosing element's Type is the ecosystem, lower-cased",
        )

    def test_an_upper_cased_ecosystem_is_lower_cased_in_the_coordinate(self) -> None:
        """The lower-casing is asserted where it can be seen, on a derived document."""
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][3])]
        document["Results"][0]["Type"] = "BUNDLER"
        document["Results"][0]["Vulnerabilities"] = [
            copy.deepcopy(document["Results"][0]["Vulnerabilities"][1])
        ]
        adapted = adapt_document(document)
        self.assertEqual(
            adapted.rows[0]["package_coordinate"],
            "bundler:rexml@3.3.8",
            msg="the ecosystem is lower-cased and the rest is taken as stated",
        )

    def test_the_enclosing_packages_fields_are_the_fourth_candidate(self) -> None:
        """Level 4, on a derived document: the record supplies neither name nor version.

        The positive fixture never reaches this level because levels 1 to 3 resolve for
        every record that gets there, so the level is exercised on a copy whose
        inventory entry carries a name and a version but no PURL.
        """
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][2])]
        element = document["Results"][0]
        element["Packages"] = [
            {"ID": "handlebars@4.5.3", "Name": "handlebars", "Version": "4.5.3"}
        ]
        record = copy.deepcopy(element["Vulnerabilities"][1])
        for member in ("PkgName", "InstalledVersion", "PkgIdentifier"):
            record.pop(member, None)
        element["Vulnerabilities"] = [record]
        adapted = adapt_document(document)
        self.assertEqual(adapted.rejections, [], msg="a coordinate was formed, so no reject")
        self.assertEqual(
            adapted.rows[0]["package_coordinate"],
            "npm:handlebars@4.5.3",
            msg="composed from the enclosing package's own fields",
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_COORDINATE_PACKAGE_FIELDS],
            1,
            msg="and counted at level 4, distinctly from the other three",
        )

    def test_a_section_that_supplies_no_coordinate_leaves_the_field_absent(self) -> None:
        """A secret and a misconfiguration name no package, and none is invented."""
        absent = 0
        for row, record in zip(self.adapted.rows, self.adapted.records):
            if record.section == "Vulnerabilities":
                continue
            with self.subTest(pointer=record.pointer):
                self.assertIsNone(
                    row["package_coordinate"],
                    msg=(
                        "absence is permitted for this field, and a finding about a "
                        "file is not a finding about a package"
                    ),
                )
                absent += 1
        self.assertEqual(
            absent,
            self.adapted.counters[trivy.COUNTER_COORDINATE_ABSENT],
            msg="the absences are counted rather than merely null",
        )

    def test_the_four_levels_and_the_absences_account_for_every_row(self) -> None:
        """The precedence counters sum to the row count: every row took exactly one route."""
        total = sum(
            self.adapted.counters[key]
            for key in (
                trivy.COUNTER_COORDINATE_RECORD_PURL,
                trivy.COUNTER_COORDINATE_PACKAGE_PURL,
                trivy.COUNTER_COORDINATE_RECORD_FIELDS,
                trivy.COUNTER_COORDINATE_PACKAGE_FIELDS,
                trivy.COUNTER_COORDINATE_ABSENT,
            )
        )
        self.assertEqual(
            total,
            len(self.adapted.rows),
            msg="one route per row, so no row's coordinate is unaccounted for",
        )


class SecretRedactionTest(TrivyAdapterTestCase):
    """No secret value reaches any dataset field, and none is in this tree.

    A secret record's ``message`` is its ``Match``, which Trivy emits **already
    redacted**, and no other member of a secret record is read -- ``Code`` carries the
    surrounding source lines and is never touched.  This tree is committed to git, whose
    only relevant ignore line matches ``artifacts/``, so the guarantee is asserted
    structurally rather than left to inspection.
    """

    def test_every_match_in_every_fixture_is_wholly_redacted(self) -> None:
        """Each ``Match`` this corpus carries is a run of asterisks and nothing else.

        Asserted by character content rather than by appearance: a value that merely
        looked redacted would be a value in a committed file.
        """
        examined = 0
        for name in required_fixture_names():
            document = load_fixture(name)
            for record in section_walk(document):
                if record.section != "Secrets":
                    continue
                match = record.record.get("Match")
                if match is None:
                    continue
                with self.subTest(fixture=name, pointer=record.pointer):
                    self.assertIsInstance(match, str)
                    self.assertEqual(
                        set(match),
                        {"*"},
                        msg="the whole value is asterisks, so it discloses nothing",
                    )
                    examined += 1
        self.assertGreater(
            examined, 0, msg="the corpus does carry redacted matches to assert over"
        )

    def test_a_secret_rows_message_is_the_redacted_match_and_no_other_member(self) -> None:
        """The message is the ``Match`` where one exists, and the ``Title`` where none does."""
        adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))
        secrets = [
            (row, record)
            for row, record in zip(adapted.rows, adapted.records)
            if record.section == "Secrets"
        ]
        self.assertEqual(len(secrets), 2, msg="both secret records are exercised")
        for row, record in secrets:
            with self.subTest(pointer=record.pointer):
                match = record.record.get("Match")
                if match is not None:
                    self.assertEqual(
                        row["message"],
                        match,
                        msg="the redacted Match is emitted as-is",
                    )
                    self.assertEqual(set(row["message"]), {"*"})
                else:
                    self.assertEqual(
                        row["message"],
                        record.record["Title"],
                        msg="and with no Match the Title is the message",
                    )

    def test_no_field_of_any_row_carries_a_secret_bearing_member(self) -> None:
        """No row field holds any textual member of a secret record beyond the ones read.

        The members this adapter reads from a secret record are ``RuleID``, ``Title``,
        ``Match``, ``Severity`` and ``StartLine``.  Every **textual** member outside that
        set is asserted absent from every field of every row, so a future record shape
        carrying a value under a new key cannot leak through a field already covered
        here.  Numeric members are excluded on purpose: ``EndLine`` legitimately equals
        ``StartLine`` on a single-line match, and a coincidence of integers is not a
        disclosure -- a secret is text.
        """
        adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))
        examined = 0
        for row, record in zip(adapted.rows, adapted.records):
            if record.section != "Secrets":
                continue
            unread = {
                key: value
                for key, value in record.record.items()
                if key not in ("RuleID", "Title", "Match", "Severity", "StartLine")
                and isinstance(value, str)
                and value
            }
            for key, value in unread.items():
                examined += 1
                for field in emit.FIELDS:
                    with self.subTest(pointer=record.pointer, member=key, field=field):
                        self.assertNotEqual(
                            row[field],
                            value,
                            msg=f"the unread member {key} reached no dataset field",
                        )
        self.assertGreater(
            examined,
            0,
            msg="the fixture's secrets do carry unread textual members to assert over",
        )

    def test_a_planted_code_excerpt_reaches_no_field(self) -> None:
        """A derived secret carrying an unread member: the row is unchanged by it.

        The planted value is an obvious placeholder rather than anything resembling a
        credential, since this assertion lives in a committed file.
        """
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][1])]
        placeholder = "REDACTED_PLACEHOLDER_NEVER_A_REAL_VALUE"
        for record in document["Results"][0]["Secrets"]:
            record["Code"] = {"Lines": [{"Content": placeholder}]}
            record["Secret"] = placeholder
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 2)
        for index, row in enumerate(adapted.rows):
            for field in emit.FIELDS:
                with self.subTest(row=index, field=field):
                    self.assertNotIn(
                        placeholder,
                        str(row[field]),
                        msg="a member the adapter does not read cannot reach a field",
                    )


class UnsupportedSectionStopsTheRunTest(TrivyAdapterTestCase):
    """A non-empty unsupported finding section stops the run, quoting what it observed.

    Why this is a stop and not a warning, stated once because it is the whole
    justification: **a silently dropped section would let reconciliation pass while real
    tool output vanished.**  The count unit of the independent counting traversal is
    ``Results[]`` x the three *supported* sections, so an ignored ``Licenses`` array is
    absent from both sides of ``raw records = rows + rejections``.  The identity would
    balance exactly -- over a count unit that never saw the dropped array -- while four
    records of real tool output left no trace in the dataset, the counters or the
    reconciliation.  A drop that unbalances the identity is caught by the identity; a
    drop invisible to both sides is caught by nothing, and an empty or silently-reduced
    result set is indistinguishable from a clean scan.  That counterfactual arithmetic
    is itself asserted below, so the reasoning is checkable rather than merely stated.

    The three deliberate non-stops are asserted too: an unsupported member that is
    present and **empty**, one that is absent, and one holding a scalar.  Validated
    empty is the requirement, not validated absent -- and an implementation that stopped
    on the members' mere presence would fail on the positive fixture, where both are
    present and empty on all six elements.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Load the fixture and its expectation once for the whole class."""
        cls.expected = load_expected("halt-trivy-unsupported-section")

    def offending_element(self, section: str) -> Any:
        """Return a copy of the fixture element whose ``section`` array is non-empty."""
        for element in load_fixture(UNSUPPORTED_SECTION_FIXTURE)["Results"]:
            if element.get(section):
                return copy.deepcopy(element)
        raise AssertionError(  # pragma: no cover - the fixture carries both
            f"blocking gap: no element of {UNSUPPORTED_SECTION_FIXTURE} holds a "
            f"non-empty {section} array, so that member cannot be covered"
        )

    def document_offending_only_in(self, section: str) -> Any:
        """Return the positive fixture with one offending element appended.

        Appending puts the offence in the **last** element, which is what establishes
        that the validation pass runs over every element rather than stopping at the
        first: a defect in the last element must stop the run as surely as one in the
        first, or a partial dataset would be produced from the elements already walked.
        """
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = document["Results"] + [self.offending_element(section)]
        return document

    def test_a_non_empty_licenses_array_stops_the_run(self) -> None:
        """Assertion 12, over the committed fixture, by the exact exception type."""
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(load_fixture(UNSUPPORTED_SECTION_FIXTURE))
        observed = self.expected["halt"]["observed_attributes"]
        error = caught.exception
        self.assertEqual(error.reason, trivy.HALT_UNSUPPORTED_SECTION)
        self.assertEqual(error.reason, observed["reason"])
        self.assertEqual(error.section, observed["section"])
        self.assertEqual(error.target, observed["target"])
        self.assertEqual(error.result_index, observed["result_index"])
        self.assertEqual(error.element_count, observed["element_count"])
        self.assertEqual(error.note, observed["note"])
        self.assertEqual(
            dict(error.structure),
            observed["structure"],
            msg="the observed structure is the expectation's, key for key",
        )

    def test_a_non_empty_experimental_modified_findings_array_stops_the_run(self) -> None:
        """Assertion 13: the second member, non-empty on its own evidence.

        In the committed fixture this array is never reached -- the ``Licenses`` element
        precedes it and raises first -- so it is exercised on a derived document that
        carries only this offence.  Recorded rather than skipped: a reader comparing the
        fixture against the stop would otherwise take the unmentioned array for one the
        validator missed.
        """
        section = "ExperimentalModifiedFindings"
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(self.document_offending_only_in(section))
        error = caught.exception
        self.assertEqual(error.section, section)
        self.assertEqual(error.reason, trivy.HALT_UNSUPPORTED_SECTION)
        self.assertEqual(
            error.element_count, 1, msg="the one object the derived array holds"
        )
        self.assertEqual(
            error.result_index,
            len(load_fixture(POSITIVE_FIXTURE)["Results"]),
            msg="the offence is in the appended last element, and was still reached",
        )

    def test_every_unsupported_section_is_covered_by_a_raising_case(self) -> None:
        """Assertion 15: iterate the constant, so a new member without a case is visible.

        A section added to :data:`trivy.UNSUPPORTED_FINDING_SECTIONS` with no fixture
        element to raise on fails here by name, rather than silently enlarging the set of
        sections nothing exercises.
        """
        covered: dict[str, int] = {}
        for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
            with self.subTest(section=section):
                with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
                    adapt_document(self.document_offending_only_in(section))
                self.assertEqual(
                    caught.exception.section,
                    section,
                    msg="the stop names the member it was raised on",
                )
                covered[section] = caught.exception.element_count or 0
        self.assertEqual(
            sorted(covered),
            sorted(trivy.UNSUPPORTED_FINDING_SECTIONS),
            msg="every member of the constant has a raising case",
        )
        for section, count in covered.items():
            with self.subTest(section=section):
                self.assertGreater(
                    count, 0, msg="and each raising case was raised on a non-empty array"
                )

    def test_an_empty_unsupported_section_does_not_stop_the_run(self) -> None:
        """Assertion 14, first half: validated empty is the requirement.

        Both members are present and empty on every element of the positive fixture, so
        the passing case is asserted there, and then on the three further empty shapes
        the adapter documents as non-stops: an empty object, a null and a scalar.  None
        of them holds a finding record to drop.
        """
        document = load_fixture(POSITIVE_FIXTURE)
        for element in document["Results"]:
            for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
                self.assertEqual(
                    element[section],
                    [],
                    msg="the fixture carries both members present and empty",
                )
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="so the validation returns rather than raising",
        )
        self.assertEqual(len(adapt_document(document).rows), 9)
        for label, value in (("empty object", {}), ("null", None), ("scalar", 0)):
            with self.subTest(shape=label):
                variant = derived(POSITIVE_FIXTURE)
                for element in variant["Results"]:
                    for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
                        element[section] = value
                self.assertIsNone(trivy.validate_finding_sections(variant, tool=TOOL))
                self.assertEqual(
                    len(adapt_document(variant).rows),
                    9,
                    msg="and every row is still emitted",
                )

    def test_an_absent_unsupported_section_does_not_stop_the_run(self) -> None:
        """Assertion 14, second half: the members are optional, and absence is ordinary."""
        document = derived(POSITIVE_FIXTURE)
        for element in document["Results"]:
            for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
                element.pop(section, None)
                self.assertNotIn(section, element)
        self.assertIsNone(trivy.validate_finding_sections(document, tool=TOOL))
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 9)
        self.assertEqual(adapted.rejections, [])

    def test_a_non_empty_object_under_an_unsupported_key_also_stops_the_run(self) -> None:
        """The member's name says it holds findings, so non-empty content of any shape stops.

        An object is not a shape Trivy emits under either key, so its contents cannot be
        claimed to have been read.  The stop carries no element count -- the value is not
        an array -- and carries a note saying so, which is how a halt report tells the two
        apart.
        """
        document = derived(POSITIVE_FIXTURE)
        document["Results"][0]["Licenses"] = {"Name": "a licence object, not an array"}
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(document)
        error = caught.exception
        self.assertEqual(error.reason, trivy.HALT_UNSUPPORTED_SECTION)
        self.assertEqual(error.section, "Licenses")
        self.assertIsNone(
            error.element_count, msg="an object has no element count to report"
        )
        self.assertTrue(error.note, msg="and the shape difference is noted instead")

    def test_the_exception_quotes_the_observed_structure(self) -> None:
        """Assertion 16: the message names the section and carries the structure excerpt."""
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(load_fixture(UNSUPPORTED_SECTION_FIXTURE))
        error = caught.exception
        message = str(error)
        self.assertIn(
            error.section,
            message,
            msg="the offending section is named in the message a report quotes",
        )
        self.assertIn(TOOL, message, msg="and so is the tool")
        self.assertIn(
            str(error.result_index),
            message,
            msg="and the element's index, so the record can be found again",
        )
        self.assertIn(
            error.structure_excerpt,
            message,
            msg="the observed structure is in the message verbatim",
        )
        self.assertTrue(error.structure, msg="and it is not empty")
        record = error.as_dict()
        self.assertEqual(record["tool"], TOOL)
        self.assertEqual(record["reason"], trivy.HALT_UNSUPPORTED_SECTION)
        self.assertEqual(record["section"], error.section)
        self.assertEqual(record["message"], message)
        json.dumps(record)  # the run record must be serialisable as it stands

    def test_the_quoted_structure_carries_keys_and_types_but_no_value(self) -> None:
        """The excerpt is shape, not content -- which is what makes it safe to publish.

        These logs are preserved verbatim, so a structure that quoted values would put
        whatever the offending records held into a published file.  Every string value of
        every offending record is asserted absent from the message, while its keys are
        asserted present.
        """
        document = load_fixture(UNSUPPORTED_SECTION_FIXTURE)
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(document)
        message = str(caught.exception)
        offending = document["Results"][caught.exception.result_index][
            caught.exception.section
        ]
        for index, record in enumerate(offending):
            for key, value in record.items():
                with self.subTest(element=index, member=key):
                    self.assertIn(
                        key, message, msg="the member's name is part of the structure"
                    )
                    if isinstance(value, str) and value:
                        self.assertNotIn(
                            value,
                            message,
                            msg="but its value is not, at any length",
                        )

    def test_the_stop_is_not_a_counted_rejection(self) -> None:
        """It is raised, never returned, and its reason is no rejection class.

        A rejection is counted under a named class and the run continues, which is
        exactly the outcome this condition rules out.  The expectation records no rows
        and no rejections for this fixture, and both are asserted empty.
        """
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(load_fixture(UNSUPPORTED_SECTION_FIXTURE))
        error = caught.exception
        self.assertNotIsInstance(error, paths.Rejection)
        self.assertIn(error.reason, trivy.HALT_REASONS)
        self.assertNotIn(error.reason, paths.REJECT_CLASSES)
        self.assertFalse(paths.is_reject_class(error.reason))
        self.assertEqual(self.expected["rows"], [], msg="the expectation states no row")
        self.assertEqual(
            self.expected["rejections"], [], msg="and no rejection either"
        )
        self.assertEqual(self.expected["outcome"], "halt")

    def test_the_counterfactual_that_a_skip_would_have_balanced(self) -> None:
        """The arithmetic a tolerated skip would have satisfied, measured rather than argued.

        The fixture's first six elements are the positive fixture's, so a skipping
        implementation would emit that fixture's nine rows, reject nothing, and satisfy
        ``9 == 9 + 0`` against a supported-section count of nine -- while the four
        records in the two unsupported arrays left no trace anywhere.  Both numbers are
        measured here, which is what makes the justification in this class's docstring a
        checkable claim rather than a stated one.
        """
        document = load_fixture(UNSUPPORTED_SECTION_FIXTURE)
        supported_records = len(section_walk(document))
        positive = adapt_document(load_fixture(POSITIVE_FIXTURE))
        self.assertEqual(
            supported_records,
            self.expected["counts"]["raw_finding_records"],
            msg="the supported-section count over this fixture is the expectation's",
        )
        self.assertEqual(
            supported_records,
            len(positive.rows) + len(positive.rejections),
            msg="so a skip would have balanced the identity exactly",
        )
        dropped = sum(
            len(element.get(section) or [])
            for element in document["Results"]
            for section in trivy.UNSUPPORTED_FINDING_SECTIONS
        )
        self.assertEqual(
            dropped,
            4,
            msg="while these four records would have vanished without trace",
        )
        self.assertEqual(
            self.expected["guarded_failure"]["arithmetic_if_the_guard_were_removed"][
                "records_that_would_vanish"
            ],
            dropped,
            msg="which is the number the expectation records for the counterfactual",
        )


def adapt_negative(stem: str) -> Adapted:
    """Adapt one negative fixture the way its own expected file prescribes.

    Six of the seven go through ``trivy.adapt``.  The unattributable-section fixture
    goes through the public seam instead, because ``adapt``'s iteration is section-bound
    by construction and cannot produce that condition -- which is a property of the
    adapter's design, not a convenience of this test.
    """
    if stem == SEAM_STEM:
        return adapt_through_seam(
            load_fixture(f"{stem}.json"),
            designated=SEAM_DESIGNATED_RECORD,
            section_key=SEAM_SECTION_KEY,
        )
    return adapt_fixture(stem)


def expected_rejection_section(entry: Any) -> Any:
    """The section a rejection was produced under, as the expected file spells it.

    The seam's expectation carries two section keys because they differ there and only
    there: the document says ``Vulnerabilities``, the call says ``Packages``, and the
    rejection reports the one it was given.
    """
    if "section_passed_to_adapt_record" in entry:
        return entry["section_passed_to_adapt_record"]
    return entry["section"]


class RejectedRecordTest(TrivyAdapterTestCase):
    """One negative fixture per rejection condition, asserted by class name and count.

    A defective record is **rejected and counted**, never dropped and never coerced into
    a row: dropping it breaks ``raw records = rows + rejections`` and coercing it puts a
    value in the dataset that this pipeline, not Trivy, decided on.  Each fixture's
    surviving records are asserted to still become rows, so a condition that rejected
    everything would fail here rather than look like a pass.

    The class is what carries each assertion.  A test that only counted rejections could
    not tell one condition from another, so every class is compared against a literal
    member of :data:`normalize.paths.REJECT_CLASSES` taken from the module.
    """

    def test_every_negative_fixture_emits_exactly_its_expected_rows(self) -> None:
        """The surviving records still become rows, field for field."""
        for stem in REJECT_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                adapted = adapt_negative(stem)
                expected = load_expected(stem)
                self.assertRowsEqualExpected(
                    adapted.rows, expected["rows"], where=stem
                )
                self.assertEqual(
                    len(adapted.rows),
                    expected["counts"]["rows"],
                    msg="and the count the expectation states in its counts block",
                )
                self.assertGreater(
                    len(adapted.rows),
                    0,
                    msg=(
                        "a negative fixture that rejected everything would assert "
                        "nothing about the records it should have kept"
                    ),
                )

    def test_every_negative_fixture_counts_its_rejections_under_the_named_class(
        self,
    ) -> None:
        """The class, the count, and the absence of every other class.

        Comparing the whole per-class mapping asserts both that the condition fired and
        that no other condition did -- which a bare rejection count could not.
        """
        for stem in REJECT_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                adapted = adapt_negative(stem)
                expected = load_expected(stem)
                self.assertEqual(
                    adapted.rejections_by_class,
                    expected["counts"]["rejections_by_class"],
                    msg="the classes and their counts, with no other class present",
                )
                self.assertEqual(
                    len(adapted.rejections),
                    expected["counts"]["rejections"],
                    msg="and the total the expectation states",
                )
                expected_class = REJECT_CLASS_BY_STEM[stem]
                self.assertEqual(
                    sorted(adapted.rejections_by_class),
                    [expected_class],
                    msg=f"{stem} exercises exactly {expected_class!r}",
                )
                self.assertEqual(
                    expected_class,
                    getattr(paths, f"REJECT_{expected_class.upper()}"),
                    msg=(
                        "the class asserted is the module's own constant, not a string "
                        "spelled by hand in this test"
                    ),
                )

    def test_every_rejection_names_its_record_and_retains_its_diagnostic(self) -> None:
        """Each rejection identifies the record it came from and says why, in words.

        The identity is enough to find the record in the artifact again and is never the
        record itself: a rejection carries no dataset fields and no value from the
        record, which is what keeps a secret out of a record that is written to the run's
        own log.
        """
        for stem in REJECT_FIXTURE_STEMS:
            adapted = adapt_negative(stem)
            expected = load_expected(stem)
            self.assertEqual(
                len(adapted.rejections),
                len(expected["rejections"]),
                msg=f"{stem}: one expectation per rejection",
            )
            for index, (rejection, entry) in enumerate(
                zip(adapted.rejections, expected["rejections"])
            ):
                with self.subTest(fixture=stem, rejection=index):
                    self.assertIsInstance(rejection, paths.Rejection)
                    self.assertEqual(rejection.tool, TOOL)
                    self.assertEqual(rejection.reject_class, entry["reject_class"])
                    self.assertIsInstance(rejection.detail, str)
                    self.assertTrue(
                        rejection.detail,
                        msg="an empty detail is the catch-all AAP 0.5.4 forbids",
                    )
                    identity = dict(rejection.record_identity)
                    self.assertEqual(
                        identity["result_index"],
                        entry["result_index"],
                        msg="the enclosing element's index",
                    )
                    self.assertEqual(
                        identity["record_index"],
                        entry["record_index"],
                        msg="the record's index within its section",
                    )
                    self.assertEqual(
                        identity["section"],
                        expected_rejection_section(entry),
                        msg="and the section the rejection was produced under",
                    )
                    self.assertIn(
                        "Target",
                        identity,
                        msg="with the artifact's own spelling of the Target",
                    )
                    json.dumps(rejection.as_dict())

    def test_no_rejected_record_became_a_row_and_none_was_dropped(self) -> None:
        """The per-artifact identity holds over every negative fixture."""
        for stem in REJECT_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                adapted = adapt_negative(stem)
                expected = load_expected(stem)
                self.assertOneOutcomePerRecord(adapted, where=stem)
                self.assertEqual(
                    adapted.raw_records,
                    expected["counts"]["raw_finding_records"],
                    msg="the record count is the expectation's count unit",
                )
                self.assertEqual(
                    len(adapted.rows) + len(adapted.rejections),
                    expected["counts"]["rows"] + expected["counts"]["rejections"],
                    msg="rows plus rejections is what the expectation states",
                )

    def test_the_counters_are_the_expected_files_for_every_negative_fixture(self) -> None:
        """Every counter of every negative fixture, key by key."""
        for stem in REJECT_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                self.assertCountersEqualExpected(
                    adapt_negative(stem), load_expected(stem), where=stem
                )

    def test_the_rejected_records_own_section_still_decided_a_class_where_it_had_one(
        self,
    ) -> None:
        """A rejected record still has a section; what it does not have is a row.

        The class is bound at step 2 of the classification order, before the identifier,
        the message, the path, the line and the coordinate -- so every rejection except
        the unattributable one carries the section its class would have come from.
        """
        for stem in REJECT_FIXTURE_STEMS:
            if stem == SEAM_STEM:
                continue
            with self.subTest(fixture=stem):
                adapted = adapt_negative(stem)
                for rejection in adapted.rejections:
                    section = rejection.record_identity["section"]
                    self.assertIn(
                        section,
                        trivy.SUPPORTED_SECTIONS,
                        msg="the record was read from a supported section",
                    )
                    self.assertIsNotNone(
                        trivy.scanner_class_for_section(section),
                        msg="whose class was never in doubt",
                    )

    def test_the_unattributable_branch_is_reached_only_through_the_seam(self) -> None:
        """The same record is a row under its own section and a rejection under any other.

        This is the falsifying pair the condition needs.  ``adapt`` over the same
        document produces no rejection at all, because its iteration is section-bound;
        handing the record its own section produces a ``vuln`` row; handing it a key
        outside the three produces the rejection.  An implementation that classed the
        record from its ``RuleID``, its ``ID``, its ``Match``, its ``CauseMetadata`` or
        the enclosing element's ``Class`` would fail this.
        """
        env = environment()
        document = load_fixture(f"{SEAM_STEM}.json")
        result_index, document_section, record_index = SEAM_DESIGNATED_RECORD
        element = document["Results"][result_index]
        record = element[document_section][record_index]

        section_bound = adapt_document(load_fixture(f"{SEAM_STEM}.json"))
        self.assertEqual(
            section_bound.rejections,
            [],
            msg="adapt cannot reach the branch: every record arrives with a real section",
        )
        self.assertEqual(len(section_bound.rows), section_bound.raw_records)

        def hand_over(section: Any) -> Any:
            """Hand the designated record to the seam under ``section``."""
            return trivy.adapt_record(
                copy.deepcopy(record),
                section=section,
                target=element.get("Target"),
                tool=TOOL,
                root=env.root,
                tool_base=env.tool_base(),
                globs=env.globs,
                tally=severity.LiteralTally.with_all_tools(),
                counters=trivy.new_counters(),
                packages=element.get("Packages") or (),
                ecosystem=element.get("Type"),
                result_index=result_index,
                record_index=record_index,
            )

        own = hand_over(document_section)
        self.assertNotIsInstance(
            own, paths.Rejection, msg="under its own section the record is a row"
        )
        self.assertEqual(own["scanner_class"], "vuln")

        for foreign in (
            SEAM_SECTION_KEY,
            "Class",
            "vulnerabilities",
            *trivy.UNSUPPORTED_FINDING_SECTIONS,
            None,
            7,
        ):
            with self.subTest(section=foreign):
                outcome = hand_over(foreign)
                self.assertIsInstance(
                    outcome,
                    paths.Rejection,
                    msg="under any other key it is a counted rejection, never a row",
                )
                self.assertEqual(
                    outcome.reject_class, paths.REJECT_UNATTRIBUTABLE_SECTION
                )

    def test_the_seams_rejection_carries_the_four_key_identity_and_its_reason(self) -> None:
        """Attribution precedes the identifier step, so the identity carries no ``rule_id``.

        The detail says what happened and, crucially, that record content is never used
        to establish a class -- the sentence a reader needs in order to see that the
        rejection is a design decision rather than a lookup that failed.
        """
        adapted = adapt_negative(SEAM_STEM)
        self.assertEqual(len(adapted.rejections), 1)
        rejection = adapted.rejections[0]
        self.assertEqual(
            sorted(rejection.record_identity),
            sorted(("result_index", "section", "record_index", "Target")),
            msg="four keys, and no rule_id: step 2 precedes step 3",
        )
        self.assertEqual(rejection.record_identity["section"], SEAM_SECTION_KEY)
        self.assertIn("is not one of this artifact's finding sections", rejection.detail)
        self.assertIn(
            "record content is never used to establish one", rejection.detail
        )

    def test_the_unmerged_part_is_read_with_the_base_its_shape_requires(self) -> None:
        """The three readings of the per-directory part, and why only one asserts anything.

        The metadata records that the retained parts are not root-anchored, and
        ``paths.resolve_trivy_path`` requires a ``per_section_target`` base with a section
        base for one.  Measuring all three readings is what shows the chosen one is the
        metadata's rather than the one that happens to produce a rejection: read against
        the scan root the fixture yields four rows and no rejection at all, and with no
        section base every record fails for the same reason and none survives.
        """
        stem = "reject-trivy-unresolvable-path"
        self.assertIn(stem, PER_SECTION_TARGET_STEMS)
        document = load_fixture(f"{stem}.json")
        section_base = document["ArtifactName"]
        self.assertNotEqual(
            section_base,
            ".",
            msg="the part states its own scope directory, not the merged '.'",
        )

        prescribed = adapt_document(
            load_fixture(f"{stem}.json"),
            base_kind=BASE_KIND_PART,
            section_base=section_base,
        )
        self.assertEqual(len(prescribed.rows), 3)
        self.assertEqual(len(prescribed.rejections), 1)
        self.assertEqual(
            prescribed.rejections[0].reject_class, paths.REJECT_UNRESOLVABLE_PATH
        )

        without_base = adapt_document(
            load_fixture(f"{stem}.json"), base_kind=BASE_KIND_PART
        )
        self.assertEqual(
            without_base.rows,
            [],
            msg="with no section base the Target half rejects every record",
        )
        self.assertEqual(len(without_base.rejections), without_base.raw_records)

        as_merged = adapt_document(load_fixture(f"{stem}.json"))
        self.assertEqual(
            len(as_merged.rejections),
            0,
            msg=(
                "and read against the scan root it rejects nothing, silently dropping "
                "the ArtifactName prefix -- the reading the metadata's note exists to "
                "prevent"
            ),
        )
        self.assertEqual(len(as_merged.rows), as_merged.raw_records)

    def test_the_unresolvable_rejection_is_the_per_record_half_of_the_resolution(
        self,
    ) -> None:
        """The Target resolved; the per-record path had no base to resolve against.

        Both halves are named in the detail, which is what distinguishes this from a
        record whose path merely resolved somewhere unwanted -- that one is a kept row
        with ``in_scope`` false.
        """
        adapted = adapt_negative("reject-trivy-unresolvable-path")
        rejection = adapted.rejections[0]
        self.assertEqual(rejection.reject_class, paths.REJECT_UNRESOLVABLE_PATH)
        self.assertIn(
            paths.PATH_BASE_KIND_PER_SECTION_TARGET,
            rejection.detail,
            msg="the detail names the base kind that supplied no base",
        )
        self.assertIn(
            "supplies no base",
            rejection.detail,
            msg="and says so in words a reader can act on",
        )


class KeptRowTest(TrivyAdapterTestCase):
    """Rows that are kept rather than rejected, asserted so a tidy-up cannot drop them.

    A path that resolves somewhere the allowlist does not cover is **not** a defect: a
    runner legitimately reaching a manifest outside the twelve roots produces a row with
    ``in_scope: false``, and that row is kept and counted (AAP 0.3.2, AAP 0.9.3).  The
    same holds for a non-filesystem coordinate.  Only evidence about the *runner*
    establishes a wrong scan root; an individual coordinate outside the tree is a
    coordinate, not a targeting fault.

    ``in_scope`` is decided by the allowlist alone.  What a runner read is a separate
    question, and this adapter filters nothing on either basis.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the positive fixture once for the whole class."""
        cls.adapted = adapt_document(load_fixture(POSITIVE_FIXTURE))

    def test_a_manifest_outside_the_twelve_globs_is_kept_with_in_scope_false(self) -> None:
        """Assertion 17: four rows from two real lockfiles, kept and marked out of scope.

        ``dev/package-lock.json`` and ``docs/Gemfile.lock`` are real files in the pinned
        tree and outside the twelve authoritative globs.  Nothing in scope resolves to a
        package -- exactly one manifest-shaped file is in scope and it carries no
        dependencies block -- so a vulnerability row from a manifest the runner reached
        outside the globs is the realistic case rather than an edge one.
        """
        env = environment()
        out_of_scope = [row for row in self.adapted.rows if not row["in_scope"]]
        self.assertEqual(
            len(out_of_scope),
            4,
            msg="four rows are out of scope, and all four are present rather than dropped",
        )
        self.assertEqual(
            sorted({row["path"] for row in out_of_scope}),
            ["dev/package-lock.json", "docs/Gemfile.lock"],
            msg="the two lockfiles outside the twelve globs",
        )
        for index, row in enumerate(out_of_scope):
            with self.subTest(row=index, path=row["path"]):
                self.assertIsNone(
                    paths.matches_any_glob(row["path"], env.globs),
                    msg="the path matches no glob, which is why in_scope is false",
                )
                self.assertFalse(
                    paths.in_scope(row["path"], env.globs),
                    msg="and the scope predicate itself agrees",
                )
                self.assertRowSchema(row, where=f"out-of-scope row {index}")
                self.assertIsNotNone(
                    row["rule_id"],
                    msg="a kept row is a complete row, not a stub",
                )

    def test_an_in_scope_row_matched_a_glob_and_the_split_is_counted(self) -> None:
        """The other five rows match a glob, and the adapter's counters state the split."""
        env = environment()
        in_scope_rows = [row for row in self.adapted.rows if row["in_scope"]]
        self.assertEqual(len(in_scope_rows), 5)
        for index, row in enumerate(in_scope_rows):
            with self.subTest(row=index, path=row["path"]):
                self.assertIsNotNone(
                    paths.matches_any_glob(row["path"], env.globs),
                    msg="an in-scope row names a path one of the twelve globs covers",
                )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_ROWS_IN_SCOPE], len(in_scope_rows)
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_ROWS_OUT_OF_SCOPE],
            len(self.adapted.rows) - len(in_scope_rows),
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_ROWS_IN_SCOPE]
            + self.adapted.counters[trivy.COUNTER_ROWS_OUT_OF_SCOPE],
            len(self.adapted.rows),
            msg="every row is on one side of the split and none is unaccounted for",
        )

    def test_the_scope_predicate_agrees_with_every_row_the_adapter_emitted(self) -> None:
        """``in_scope`` is the allowlist's verdict on the emitted path, on every row."""
        env = environment()
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index, path=row["path"]):
                self.assertEqual(
                    row["in_scope"],
                    paths.in_scope(row["path"], env.globs),
                    msg="decided by the allowlist alone, and by nothing else",
                )

    def test_an_archive_member_is_serialized_with_one_separator_and_kept(self) -> None:
        """Assertion 18: ``<container-relative-to-root>!<member>``, kept and counted.

        Derived, because no Target in the committed fixture names a container -- the
        fixture's ``Node.js`` is an ecosystem label and ``.js`` is not an archive
        extension, so nothing there produces this serialization.  The container is a real
        in-tree path under one of the twelve globs, which is what makes the ``in_scope``
        result attributable to the coordinate's **kind** rather than to the container
        being out of scope.
        """
        container = "python/pyspark/lib/example-archive.zip"
        member = "example/module.py"
        self.assertIsNotNone(
            paths.matches_any_glob(container, environment().globs),
            msg="the container itself is inside the twelve globs",
        )
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][4])]
        element = document["Results"][0]
        element["Target"] = container
        element["Vulnerabilities"][0]["PkgPath"] = member
        adapted = adapt_document(document)

        self.assertEqual(len(adapted.rows), 1, msg="the row is kept, not rejected")
        self.assertEqual(adapted.rejections, [])
        row = adapted.rows[0]
        self.assertEqual(
            row["path"],
            f"{container}{paths.ARCHIVE_SEPARATOR}{member}",
            msg="the container relative to the root, one separator, then the member",
        )
        self.assertEqual(
            row["path"].count(paths.ARCHIVE_SEPARATOR),
            1,
            msg="'!' is the single separator between container and member",
        )
        self.assertRelativePath(row["path"], where="archive-member row")
        self.assertFalse(
            row["in_scope"],
            msg=(
                "a non-filesystem coordinate is never in scope, and the rule is applied "
                "before the globs: on its segments alone this path would match one"
            ),
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_NON_FILESYSTEM_PATHS],
            1,
            msg="and it is counted, so the reported proportion can include it",
        )
        self.assertEqual(
            adapted.counters[
                f"{trivy.COUNTER_PATH_KIND_PREFIX}{paths.PATH_KIND_ARCHIVE_MEMBER}"
            ],
            1,
            msg="under the archive-member kind rather than as a tree file",
        )
        self.assertTrue(
            paths.is_non_filesystem_kind(paths.PATH_KIND_ARCHIVE_MEMBER),
            msg="which paths.py classifies as a non-filesystem coordinate",
        )

    def test_a_path_inside_a_test_tree_is_kept_with_in_scope_false(self) -> None:
        """The ``src/test`` exclusion is literal, and it excludes without dropping.

        A path containing ``src/test`` is out of scope and the exclusion overrides a
        positive glob match -- but the row is kept, exactly as a row outside the globs is
        kept.  Asserted here because "out of scope" and "dropped" are the two readings
        this dataset must never conflate.
        """
        env = environment()
        # Under python/pyspark/**, so the glob matches and the exclusion is what decides.
        # A Scala test tree such as core/src/test/... would be out of scope for a second
        # reason -- no glob covers it at all -- and would therefore establish nothing
        # about which of the two rules did the work.
        target = "python/pyspark/example/src/test/example_module.py"
        self.assertTrue(
            paths.contains_src_test(target),
            msg="the path does carry the literal marker",
        )
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][1])]
        document["Results"][0]["Target"] = target
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 2, msg="both records are kept")
        self.assertEqual(adapted.rejections, [])
        for index, row in enumerate(adapted.rows):
            with self.subTest(row=index):
                self.assertEqual(row["path"], target)
                self.assertFalse(
                    row["in_scope"],
                    msg="the exclusion overrides the glob match that would otherwise hold",
                )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_ROWS_OUT_OF_SCOPE],
            2,
            msg="and both are counted out of scope rather than removed",
        )
        self.assertIsNotNone(
            paths.matches_any_glob(target, env.globs),
            msg="the glob would have matched, which is what makes the override the rule",
        )

    def test_a_target_that_names_no_file_is_still_expressed_against_the_root(self) -> None:
        """The fixture's ``Node.js`` Target names an ecosystem, and no row escapes the root.

        Its record refines the Target with a ``PkgPath``, so the emitted path is the
        manifest that package was found in -- a real in-scope file.  What is asserted here
        is the invariant that holds regardless: no emitted path is absolute, and none is
        the ecosystem label itself.
        """
        ecosystem_targets = [
            record for record in self.adapted.records if record.target == "Node.js"
        ]
        self.assertEqual(
            len(ecosystem_targets), 1, msg="the fixture carries one such Target"
        )
        row = self.adapted.rows[self.adapted.records.index(ecosystem_targets[0])]
        self.assertEqual(
            row["path"],
            "core/src/main/resources/org/apache/spark/ui/static/package.json",
            msg="the per-record path refined a Target that names no file",
        )
        self.assertTrue(
            row["in_scope"],
            msg="and that manifest is the one manifest-shaped file inside the globs",
        )
        self.assertFalse(
            paths.looks_like_archive_container("Node.js"),
            msg="'.js' is not an archive extension, so no member serialization arises",
        )
        for index, candidate in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                self.assertNotEqual(
                    candidate["path"],
                    "Node.js",
                    msg="an ecosystem label is never emitted as a path",
                )


if __name__ == "__main__":  # pragma: no cover - exercised through unittest discovery
    unittest.main(verbosity=2)
